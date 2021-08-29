package main

import (
	"io"
	"sync/atomic"

	"github.com/as/log"
)

func init() {
	log.Service = "ccp"
}

var iostat = struct {
	rx, tx int64
}{}

type rx struct{ io.Reader }

func (r rx) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	atomic.AddInt64(&iostat.rx, int64(n))
	return n, err
}

type tx struct{ io.Writer }

func (w tx) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	atomic.AddInt64(&iostat.tx, int64(n))
	return n, err
}
