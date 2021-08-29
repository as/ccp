package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
)

var ctx = context.Background()

var driver = map[string]interface {
	Open(string) (io.ReadCloser, error)
	Create(string) (io.WriteCloser, error)
}{
	"s3":   &S3{ctx: ctx},
	"gs":   &GS{ctx: ctx},
	"file": &OS{},
	"":     &OS{},
}

func main() {
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
	dst, err := dfs.Create(a[1])
	ck("create dst", err)

	buf := make([]byte, 1024*1024*64)
	_, err = io.CopyBuffer(dst, src, buf)
	ck("copy", err)

}

func ck(c string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v", c, err)
		os.Exit(1)
	}
}

type OS struct {
}

func (f OS) Open(file string) (io.ReadCloser, error) {
	return os.Open(file)
}
func (f OS) Create(file string) (io.WriteCloser, error) {
	return os.Create(file)
}

func uri(s string) url.URL {
	u, _ := url.Parse(s)
	if u == nil {
		return url.URL{}
	}
	return *u
}
