package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
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

func main() {
	defer closeAll()

	flag.Parse()
	a := flag.Args()
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
	src, err := sfs.Open(a[0])
	ck("open src", err)
	defer src.Close()

	dst, err := dfs.Create(a[1])
	ck("create dst", err)
	defer dst.Close()

	done := make(chan bool)
	go func() {
		buf := make([]byte, 1024*1024*64)
		_, err = io.CopyBuffer(tx{dst}, rx{src}, buf)
		ck("copy", err)
		close(done)
	}()

	tick := time.NewTicker(time.Second).C
Loop:
	for {
		select {
		case <-done:
			rx := atomic.LoadInt64(&iostat.rx)
			tx := atomic.LoadInt64(&iostat.tx)
			log.Info.Add("src", a[0], "dst", a[1], "rx", rx, "tx", tx).Printf("done")
			break Loop
		case <-tick:
			rx := atomic.LoadInt64(&iostat.rx)
			tx := atomic.LoadInt64(&iostat.tx)
			log.Info.Add("src", a[0], "dst", a[1], "rx", rx, "tx", tx).Printf("")
		}
	}
}

func ck(c string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v", c, err)
		os.Exit(1)
	}
}

func uri(s string) url.URL {
	u, _ := url.Parse(s)
	if u == nil {
		return url.URL{}
	}
	return *u
}
