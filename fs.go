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
	bs    = flag.Int("bs", 16*1024*1024, "block size for copy operation")
	dry   = flag.Bool("dry", false, "print (and unroll) ccp commands only; no I/O ops")
	ls    = flag.Bool("ls", false, "list the source files in dirs with their file sizes")
	quiet = flag.Bool("q", false, "dont print any progress output")
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
			ec <- fmt.Errorf("open src: %w", err)
			return
		}

		dst, err := dfs.Create(dst)
		if err != nil {
			ec <- fmt.Errorf("create dst: %w", err)
			return
		}
		defer dst.Close()
		defer src.Close()

		buf := make([]byte, *bs)
		_, err = io.CopyBuffer(tx{dst}, rx{src}, buf)
		ec <- err
	}
}

func list(src ...string) {
	for _, src := range src {
		sfs := driver[uri(src).Scheme]
		if sfs == nil {
			println("src: scheme not supported", src)
			os.Exit(1)
		}
		dir, err := sfs.List(src)
		if err != nil {
			log.Error.F("list error: %q: %v", src, err)
			continue
		}
		for _, f := range dir {
			fmt.Println(f.Size, f.Path)
		}
	}
}

func main() {
	defer closeAll()

	flag.Parse()
	a := flag.Args()
	if *ls {
		list(a...)
		os.Exit(0)
	}
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
	list, err := sfs.List(a[0])
	if err != nil {
		log.Error.Add("action", "list", "err", err).Printf("")
	}
	ec := make(chan error)
	n := 0
	for _, src := range list {
		dst := src2dst(a[0], src.String(), a[1])
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

	tick := time.NewTicker(time.Second).C

	for i := 0; i < n; {
		select {
		case err := <-ec:
			i++
			if err != nil {
				nerr++
				log.Error.Add("err", err).Printf("copy error")
			}
		case <-tick:
			progress(i, n)
		}
	}
	progress(n, n)
	if nerr != 0 {
		os.Exit(nerr)
	}
}

var nerr = 0
var txquota = 0
var procstart = time.Now()

func progress(done, total int) {
	if *quiet {
		return
	}
	rx := atomic.LoadInt64(&iostat.rx)
	tx := atomic.LoadInt64(&iostat.tx)
	dur := time.Since(procstart)
	bps := int64(0)
	if s := dur / time.Second; s > 0 {
		bps = tx / int64(s)
	}
	if txquota == 0 {
		txquota = -1
	}
	log.Info.Add(
		"rx", rx,
		"tx", tx,
		"total", txquota,
		"file.done", done,
		"file.total", total,
		"file.errors", nerr,
		"mbps", bps/(1024*1024),
		"progress", tx*100/int64(txquota),
		"runtime", dur.Seconds(),
	).Printf("")
}

func src2dst(prefix, src, dst string) url.URL {
	su := uri(src)
	du := uri(dst)
	su.Path = strings.TrimPrefix(su.Path, uri(prefix).Path)
	du.Path = path.Join(du.Path, su.Path)
	return du
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
