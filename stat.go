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
	tags := strings.Split(os.Getenv("TAGS"), ",")
	for i := 0; i+1 < len(tags); i += 2 {
		key, val := tags[i], tags[i+1]
		if key == "" || val == "" {
			continue
		}
		log.Tags = log.Tags.Add(key, val)
	}
	for _, key := range strings.Split(os.Getenv("LOGENV"), ",") {
		if val := os.Getenv(key); val != "" && key != "" {
			log.Tags = log.Tags.Add(key, val)
		}
	}
	log.Tags = log.Tags.Add("ver", Version)
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
