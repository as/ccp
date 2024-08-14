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
	if !*secure && strings.HasPrefix(su, "https") {
		// This is a lot faster than using SSL or http2
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
	Fin() // marks the writer as done
}

func calcpartsize(size int) (ps int) {
	const (
		KiB = 1024
		MiB = KiB * 1024
		GiB = MiB * 1024
		TiB = GiB * 1024
	)
	defer func() {
		if !*quiet {
			log.Info.Add("partsize", ps).Printf("chose partsize for %d MiB file", size/1024/1024)
		}
	}()
	if *partsize != 0 {
		return *partsize
	}
	if *count != 0 && *count < size {
		size = *count
	}
	switch {
	case size >= 50*GiB:
		return 512 * MiB
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
	partsize := calcpartsize(f.Len)
	nw := f.Len / partsize
	if nw == 0 {
		return fmt.Errorf("file too small")
	}
	f.BS = f.Len / nw
	if *count > 0 {
		nw = *count / partsize
		if *count%partsize != 0 {
			nw++
		}
		if nw == 0 {
			nw = 1
		}
	}
	f.Block = make([]Store, nw)
	for i := range f.Block {
		if i == 0 {
			// the first block will always be in memory
			f.Block[i] = &Block{}
		} else {
			// use disk for subsequent
			s, err := makedisk(i)
			if err != nil {
				return err
			}
			f.Block[i] = s
		}
	}
	for i := 0; i < nw; i++ {
		go f.work(dir, i)
	}
	return nil
}

func (f *File) work(dir string, block int) {
	r, _ := newHTTPRequest("GET", dir, nil)
	sp := *seek + block*f.BS
	//log.Info.Printf("sp=%d seek=%d count=%d", sp, *seek, *count)
	if sp > *seek+*count && *count > 0 {
		return
	}
	ep := sp + f.BS
	if ep > *seek+*count && *count > 0 {
		ep = *seek + *count
	}
	if ep > f.Len {
		ep = f.Len
	}
	clamp := f.BS*(block+1) - *count
	//log.Info.Printf("bs=%d block=%d ep=%d f.Len=%d clamped to read %s bytes", f.BS, block, ep, f.Len, clamp)
	if block > 0 && sema != nil {
		// NOTE(as): Sleep sort the workers so they take items from
		// the semaphore in FIFO order. Unless its block 0, then just
		// start
		time.Sleep(100 * time.Millisecond * time.Duration(block))
		sema <- true
		defer func() {
			<-sema
		}()
	}
	log.Debug.F("start %d", block)
	//log.Info.Printf("Range %s", fmt.Sprintf("bytes=%d-%d", sp, ep-1))
	r.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", sp, ep-1))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		log.Fatal.Add("err", err).F("downloading block %d", block)
	}
	body := io.Reader(resp.Body)
	if clamp > 0 {
		body = io.LimitReader(body, int64(clamp))
	}
	_, err = io.Copy(f.Block[block], body)
	if err != nil {
		log.Fatal.Add("err", err).F("downloading block %d", block)
	}
	f.Block[block].Fin()
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

	n, err = f.Block[block].ReadAt(p, int64(seek))

	f.R += int(n)
	if f.R >= f.Len {
		err = io.EOF
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
	Data []byte
	fin  bool
}

func (b *Block) Fin() {
	b.Lock()
	b.fin = true
	b.Unlock()
}

func (b *Block) Close() error {
	log.Debug.F("closing block")
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
	fd, err := os.CreateTemp(tmp, fmt.Sprintf("ccp*-%d", n))
	if err != nil {
		return nil, err
	}
	log.Debug.F("created temp file %s", fd.Name())
	tmpdir.Store(fd.Name(), true)
	return &Disk{Name: fd.Name(), File: fd}, nil
}

// Disk is disk backed storage (hopefully not ramfs)
type Disk struct {
	Name string
	*os.File
	fin int64 // if 0, still writing otherwise done
}

func (d *Disk) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = d.File.ReadAt(p, off)
	if err != nil {
		if err == io.EOF && !d.final() {
			return n, nil
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
	os.Remove(d.Name)
	tmpdir.Delete(d.Name)
	log.Debug.F("delete file %q", d.Name)
	return err
}
