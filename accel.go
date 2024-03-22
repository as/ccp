package main

import (
	"fmt"
	"github.com/as/log"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	s3 "github.com/aws/aws-sdk-go/service/s3"
)

var sema = make(chan bool, 32)

func (g *S3) Sign(uri string) (string, int, error) {
	if !g.ensure() {
		return "", 0, g.err
	}
	gc, _ := g.regionize(uri)
	u := parseURL(uri)
	sig, _ := gc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &u.Host,
		Key:    &u.Path,
	})
	su, _ := sig.Presign(72 * time.Hour)
	size, err := httpsize(su)
	if !*secure && strings.HasPrefix(su, "https") {
		su = "http" + strings.TrimPrefix(su, "https")
	}
	log.Info.F("http fast path for %d byte file %q: %v", size, uri, err)
	return su, size, err
}

func parseURL(s string) url.URL {
	if u, _ := url.Parse(s); u != nil {
		return *u
	}
	return url.URL{}
}

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

func (f *File) Download(dir string) error {
	f.Len, _ = httpsize(dir)
	if f.Len == 0 {
		return fmt.Errorf("unknown file size")
	}
	nw := f.Len / *partsize
	if nw == 0 {
		return fmt.Errorf("file too small")
	}
	f.BS = f.Len / nw
	f.Block = make([]Store, nw)
	for i := range f.Block {
		if i == 0 {
			f.Block[i] = &Block{}
		} else {
			s, err := makedisk(i)
			if err != nil {
				return err
			}
			f.Block[i] = s
		}
	}
	for i := 0; i < nw; i++ {
		go f.work(dir, i, "")
	}
	return nil
}

func (f *File) work(dir string, block int, _ string) {
	r, _ := http.NewRequest("GET", dir, nil)
	sp := block * f.BS
	ep := sp + f.BS
	if ep > f.Len {
		ep = f.Len
	}
	if block > 0 {
		// NOTE(as): Sleep sort the workers so they take items from
		// the semaphore in FIFO order.
		time.Sleep(100 * time.Millisecond * time.Duration(block))
		sema <- true
		defer func() {
			<-sema
		}()
	}
	log.Debug.F("start %d", block)
	r.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", block*f.BS, ep-1))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		panic(err)
	}
	io.Copy(f.Block[block], resp.Body)
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
		f.Block[block].Close()
	}
	return
}

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

func makedisk(n int) (*Disk, error) {
	fd, err := os.CreateTemp(*tmp, fmt.Sprintf("ccp*-%d", n))
	if err != nil {
		return nil, err
	}
	log.Debug.F("created temp file %s", fd.Name())
	return &Disk{Name: fd.Name(), File: fd}, nil
}

type Disk struct {
	Name string
	*os.File
}

func (d *Disk) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = d.File.ReadAt(p, off)
	if err != nil {
		if err == io.EOF {
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
