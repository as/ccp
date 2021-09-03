package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/as/log"
)

var ctx = context.Background()

var driver = map[string]interface {
	Open(string) (io.ReadCloser, error)
	Create(string) (io.WriteCloser, error)
	Close() error
}{
	"s3":    &S3{ctx: ctx},
	"gs":    &GS{ctx: ctx},
	"file":  &OS{},
	"http":  &HTTP{ctx: ctx},
	"https": &HTTP{ctx: ctx},
	"":      &OS{},
}

func closeAll() {
	for _, fs := range driver {
		fs.Close()
	}
}

func docp(src, dst string, ec chan<- error) {
	sfs := driver[uri(src).Scheme]
	dfs := driver[uri(dst).Scheme]

	{
		src, err := sfs.Open(src)
		if err != nil {
			ec <- fmt.Errorf("open src", err)
			return
		}
		defer src.Close()

		dst, err := dfs.Create(dst)
		if err != nil {
			ec <- fmt.Errorf("create dst", err)
			return
		}
		defer dst.Close()

		buf := make([]byte, 1024*1024*64)
		_, err = io.CopyBuffer(tx{dst}, rx{src}, buf)
		ec <- err
	}
}

func main() {
	defer closeAll()

	flag.Parse()
	a := flag.Args()
	if len(a) == 1 {
		s3 := &S3{}
		list, _ := s3.List(a[0])
		for _, fi := range list {
			fmt.Printf("ccp %q %q # %d\n", fi.Path, "dst", fi.Size)
		}
		os.Exit(0)
	}
	if len(a) != 2 {
		println("usage: ccp src dst")
		os.Exit(1)
	}
	sfs, dfs := driver[uri(a[0]).Scheme], driver[uri(a[1]).Scheme]
	if sfs == nil {
		println("src: not supported", a[0])
		os.Exit(1)
	}
	if dfs == nil {
		println("dst: not supported", a[1])
		os.Exit(1)
	}
	ec := make(chan error)
	dst := a[len(a)-1]
	n := 0
	for _, src := range a[:len(a)-1] {
		go docp(src, dst, ec)
		n++
	}

	tick := time.NewTicker(time.Second).C

	for i := 0; i < n; {
		select {
		case <-ec:
			i++
			rx := atomic.LoadInt64(&iostat.rx)
			tx := atomic.LoadInt64(&iostat.tx)
			log.Info.Add("rx", rx, "tx", tx, "progress", i*100/n).Printf("done")
		case <-tick:
			rx := atomic.LoadInt64(&iostat.rx)
			tx := atomic.LoadInt64(&iostat.tx)
			log.Info.Add("rx", rx, "tx", tx).Printf("")
		}
	}
}

func ck(c string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v", c, err)
		os.Exit(1)
	}
}

type Info struct {
	Path string
	Size int
}

func prefix(path string) string {
	n := strings.IndexAny(path, "*?[")
	if n > 0 {
		path = path[:n]
	}
	n = strings.LastIndex(path, "/")
	if n > 0 {
		path = path[:n]
	}
	return path
}

func uri(s string) url.URL {
	u, _ := url.Parse(s)
	if u == nil {
		return url.URL{}
	}
	return *u
}
