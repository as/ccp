package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/as/log"

	s3 "github.com/aws/aws-sdk-go/service/s3"
)

var sema chan bool

func sslstrip(su string, err error) (string, error) {
	if err != nil {
		log.Error.F("signer: %s: %s", su, err)
		return su, err
	}
	if *insecure && strings.HasPrefix(su, "https") {
		// This used to be a lot faster than using SSL or http2
		// but most providers just generate a redirect now instead
		// of transmitting over an insecure connection, making this
		// slower than https.
		//
		// it can only be enabled with -insecure
		su = "http" + strings.TrimPrefix(su, "https")
	}
	return su, err
}

func (g *S3) Sign(uri string) (string, error) {
	if !g.ensure() {
		return "", g.err
	}
	gc, _ := g.regionize(uri)
	u := parseURL(uri)
	sig, _ := gc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &u.Host,
		Key:    &u.Path,
	})
	su, _ := sig.Presign(72 * time.Hour)
	return sslstrip(su, nil)
}

var parseURL = uri

func (f HTTP) fastopen(file string) (io.ReadCloser, error) {
	f.ensure()
	size, err := httpsize(file)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, io.EOF
	}
	fi := File{Len: size}
	return &fi, fi.Download(file)
}

type File struct {
	Len   int
	BS, R int
	Block []Store
}

type Store interface {
	io.ReaderAt
	io.Closer
	io.Writer
	Init() error
	Ready() bool
	Fin() // marks the writer as done
}

func calcpartsize(size, hint int) (ps int) {
	const (
		KiB = 1024
		MiB = KiB * 1024
		GiB = MiB * 1024
		TiB = GiB * 1024
	)
	defer func() {
		if !*quiet {
			log.Info.Add("partsize", ps).Printf("chose %d MiB partsize for %d MiB file (%d MiB byte range)", ps/1024/1024, hint/1024/1024, size/1024/1024)
		}
	}()
	if *partsize != 0 {
		return *partsize
	}
	if *count != 0 && *count < size {
		size = *count
	}
	switch {
	case size >= 100*GiB:
		return 1024 * MiB
	case size >= 50*GiB:
		return 512 * MiB
	case size >= 10*GiB:
		return 384 * MiB
	case size >= 5*GiB:
		return 256 * MiB
	case size >= 1*GiB:
		return 128 * MiB
	case size >= 100*MiB:
		return 64 * MiB
	case size >= 100*MiB:
		return 64 * MiB
	case size >= 32*MiB:
		return 32 * MiB
	}
	return 8 * MiB
}

func (f *File) Download(dir string) error {
	f.Len, _ = httpsize(dir)
	if f.Len == 0 {
		return fmt.Errorf("unknown file size")
	}
	count := *count
	if count == 0 || count > f.Len-*seek {
		count = f.Len - *seek
	}
	partsize := calcpartsize(count, f.Len)
	nw := count / partsize
	if nw == 0 {
		return fmt.Errorf("file too small")
	}
	f.BS = count / nw
	if count%partsize != 0 {
		nw++
	}
	if nw == 0 {
		nw = 1
	}
	f.Block = make([]Store, nw)

	for i := range f.Block {
		if i == 0 && partsize <= *maxmem {
			f.Block[i] = &Block{}
			log.Debug.Add().Printf("creating memory block")
			// the first block might be in memory
		} else {
			if i == 0 {
				// unless its too big
				log.Debug.Add().Printf("partsize %d too large for memory block (maxmem=%d)", partsize, *maxmem)
			}
			// but the rest will always use the disk
			s, err := makedisk(i)
			log.Debug.Add().Printf("creating disk blocks")
			if err != nil {
				return err
			}
			f.Block[i] = s
		}
	}
	for i := 0; i < nw; i++ {
		i := i
		go f.work(dir, i)
	}
	return nil
}

func (f *File) work(dir string, block int) {
	defer func() {
		f.Block[block].Fin()
	}()
	r, _ := newHTTPRequest("GET", dir, nil)
	sp := *seek + block*f.BS
	log.Debug.Printf("sp=%d seek=%d count=%d", sp, *seek, *count)
	count := *count
	if *seek+count > f.Len || count == 0 {
		count = f.Len
	}
	if sp > *seek+count && count > 0 {
		return
	}
	ep := sp + f.BS
	if ep > *seek+count {
		ep = *seek + count
	}
	if ep > f.Len {
		ep = f.Len
	}
	if sp >= ep {
		return
	}
	clamp := ep - sp
	log.Debug.Printf("bs=%d block=%d sp=%d ep=%d f.Len=%d clamped to byte %d", f.BS, block, sp, ep, f.Len, clamp)
	if block > 0 && sema != nil {
		// NOTE(as): Sleep sort the workers so they take items from
		// the semaphore in FIFO order. Unless its block 0, then just
		// start
		if !*nosort {
			time.Sleep(200 * time.Millisecond * time.Duration(block))
		}
		sema <- true
		defer func() {
			<-sema
		}()
	}
	log.Debug.F("block %d: start range %s", block, fmt.Sprintf("bytes=%d-%d", sp, ep-1))
	r.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", sp, ep-1))
	resp, err := http.DefaultClient.Do(r)
	if *debug {
		logopen("fastopen", dir, resp, err)
	}
	if err != nil {
		log.Fatal.Add("err", err).F("downloading block %d", block)
	}
	body := io.Reader(resp.Body)
	if clamp > 0 {
		log.Debug.F("block %d: limiting read to %d bytes", block, clamp)
		body = io.LimitReader(body, int64(clamp))
	}
	f.Block[block].Init()
	n, err := io.Copy(f.Block[block], body)
	log.Debug.F("block %d: read %d bytes", block, n)
	if err != nil {
		log.Fatal.Add("err", err).F("downloading block %d", block)
	}
	resp.Body.Close()
}

func (f *File) Close() (err error) {
	return
}

func (f *File) Read(p []byte) (n int, err error) {
	block := f.R / f.BS
	seek := f.R % f.BS
	if block >= len(f.Block) {
		return 0, io.EOF
	}
	if !f.Block[block].Ready() {
		return 0, nil
	}

	n, err = f.Block[block].ReadAt(p, int64(seek))
	f.R += int(n)
	if err == io.EOF && f.R/f.BS > block {
		// its only a true EOF if the read advanced beyond
		// the block; because blocks already detect the condition
		// of data waiting to be written and return nil instead of EOF
		err = nil
	}
	if err != nil {
		log.Debug.F("read block %d@%d and read %d bytes err=%v", block, seek, n, err)
	}
	next := f.R / f.BS
	if next > block {
		// We only support one reader, if the next read
		// advances past this block, we can dispose of it
		// to free memory or disk space since nothing will
		// read it again
		f.Block[block].Close()
	}
	return
}

// Block is memory backed data
type Block struct {
	sync.Mutex
	Data  []byte
	fin   bool
	ready int64
}

func (b *Block) Init() (err error) {
	b.Lock()
	b.fin = false
	b.Data = b.Data[:0]
	b.Unlock()
	atomic.AddInt64(&b.ready, +1)
	return nil
}

func (b *Block) Ready() bool {
	return atomic.LoadInt64(&b.ready) != 0
}

func (b *Block) Fin() {
	log.Debug.F("memory block mark final")
	b.Lock()
	b.fin = true
	b.Unlock()
}

func (b *Block) Close() error {
	log.Debug.F("closing memory block")
	b.Data = nil
	return nil
}

func (b *Block) Write(p []byte) (n int, err error) {
	b.Lock()
	b.Data = append(b.Data, p...)
	b.Unlock()
	return len(p), nil
}

func (b *Block) ReadAt(p []byte, off int64) (n int, err error) {
	at := int(off)
	b.Lock()
	defer b.Unlock()
	if at >= len(b.Data) {
		if b.fin {
			return 0, io.EOF
		}
		return 0, nil
	}
	return copy(p, b.Data[at:]), nil
}

var tmpctr int64

func makedisk(n int) (*Disk, error) {
	tmp := temps[atomic.AddInt64(&tmpctr, +1)%int64(len(temps))]
	log.Debug.Add().Printf("makedisk: %d: selected temp folder: %s", n, tmp)
	d := &Disk{}
	d.init = func() error {
		fd, err := os.CreateTemp(tmp, fmt.Sprintf("ccp%s-%04d", "*", n))
		if err != nil {
			log.Error.Add().Printf("block %d: failed to create temp file in: %s: %s", n, tmp, err)
			return err
		}
		d.Name = fd.Name()
		d.File = fd
		log.Debug.F("created temp file %s", fd.Name())
		tmpdir.Store(fd.Name(), true)
		return nil
	}
	return d, nil
}

// Disk is disk backed storage (hopefully not ramfs)
type Disk struct {
	Name string
	init func() error
	*os.File
	fin   int64 // if 0, still writing otherwise done
	ready int64
}

func (d *Disk) Ready() bool {
	return atomic.LoadInt64(&d.ready) != 0
}

func (d *Disk) Init() (err error) {
	defer atomic.AddInt64(&d.ready, +1)
	err = d.init()
	if err != nil {
		return fmt.Errorf("disk init: %w", err)
	}
	return nil
}

func (d *Disk) ReadAt(p []byte, off int64) (n int, err error) {
	if !d.Ready() {
		panic("readat before init")
	}
	retry := 10
Read:
	n, err = d.File.ReadAt(p, off)
	if n == 0 && err == nil {
		quantum() // avoid spinning in a read loop
	}
	if err != nil {
		if err == io.EOF && !d.final() {
			// fake eof because the writer is still writing from
			// a slow connection, when its done this EOF will
			// be correct

			quantum() // avoid spinning in a read loop
			return n, nil
		}
		log.Debug.F("read @%d: err: %v and final=%v", off, err, d.final())
		if retry != 0 {
			retry--
			goto Read
		}
	}
	return
}

func (d *Disk) Fin() {
	atomic.AddInt64(&d.fin, +1)
}

func (d *Disk) final() bool {
	return atomic.LoadInt64(&d.fin) != 0
}

func (d *Disk) Close() error {
	err := d.File.Close()
	if !*nogc {
		os.Remove(d.Name)
		tmpdir.Delete(d.Name)
		log.Debug.F("delete file %q", d.Name)
	}
	return err
}
