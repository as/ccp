package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/as/log"
)

var (
	bs  = flag.Int("bs", 64*1024*1024, "block size for copy operation")
	dry = flag.Bool("dry", false, "print (and unroll) ccp commands only; no I/O ops")
)

var (
	errNotImplemented = errors.New("not yet implemented")
)

var ctx = context.Background()

var driver = map[string]interface {
	List(string) ([]Info, error)
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

		buf := make([]byte, *bs)
		_, err = io.CopyBuffer(tx{dst}, rx{src}, buf)
		ec <- err
	}
}

func main() {
	defer closeAll()

	flag.Parse()
	a := flag.Args()
	if len(a) != 2 {
		println("usage: ccp src... dst")
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
	list, _ := sfs.List(a[0])
	ec := make(chan error)
	n := 0
	for _, src := range list {
		dst := uri(a[1])
		dst.Path = path.Join(dst.Path, src.Path)
		if *dry {
			fmt.Printf("ccp %q %q # %d\n", src, dst.String(), src.Size)
		} else {
			txquota += src.Size
			go docp(src.String(), dst.String(), ec)
			n++
		}
	}
	if *dry {
		os.Exit(0)
	}

	nerr := 0
	tick := time.NewTicker(time.Second).C
	for i := 0; i < n; {
		select {
		case err := <-ec:
			i++
			if err != nil {
				nerr++
			}
			log.Info.Add("err", err, "total", n, "done", i).Printf("")
		case <-tick:
			progress()
		}
	}
	progress()
	os.Exit(nerr)
}

var txquota = 0

func progress() {
	rx := atomic.LoadInt64(&iostat.rx)
	tx := atomic.LoadInt64(&iostat.tx)
	log.Info.Add("rx", rx, "tx", tx, "quota", txquota, "progress", tx*100/int64(txquota)).Printf("")
}

func ck(c string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v", c, err)
		os.Exit(1)
	}
}

type Info struct {
	// invariant: *url.URL is never nil
	*url.URL
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
