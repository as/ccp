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
}

func calcpartsize(size int) (ps int) {
	defer func() {
		log.Info.Add("partsize", ps).Printf("chose partsize for %d MiB file", size/1024/1024)
	}()
	if *partsize != 0 {
		return *partsize
	}
	switch {
	case size >= 50*1024*1024*1024:
		return 512 * 1024 * 1024
	case size >= 5*1024*1024*1024:
		return 256 * 1024 * 1024
	case size >= 1024*1024*1024:
		return 128 * 1024 * 1024
	case size >= 100*1024*1024:
		return 64 * 1024 * 1024
	}
	return 32 * 1024 * 1024
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
	r.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", sp, ep-1))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		log.Fatal.Add("err", err).F("downloading block %d", block)
	}
	_, err = io.Copy(f.Block[block], resp.Body)
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
	return &Disk{Name: fd.Name(), File: fd}, nil
}

// Disk is disk backed storage (hopefully not ramfs)
type Disk struct {
	Name string
	*os.File
}

func (d *Disk) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = d.File.ReadAt(p, off)
	if err != nil {
		if err == io.EOF {
			// This file has no way of knowing if its really EOF or not
			// if the writer is slow, it will say this but the writer might
			// still be doing writing in between.
			return n, nil
		}
	}
	return
}

func (d *Disk) Close() error {
	err := d.File.Close()
	os.Remove(d.Name)
	log.Debug.F("delete file %q", d.Name)
	return err
}
