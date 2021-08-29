package main

import (
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/as/log"
)

func init() {
	log.Service = os.Getenv("SVC")
	for _, t := range strings.Split(os.Getenv("TAGS"), ",") {
		log.Tags = log.Tags.Add(strings.TrimSpace(t))
	}
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
