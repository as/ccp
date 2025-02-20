package main

import (
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

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
	if n == 0 && err == nil {
		quantum() // avoid spinning in a read loop
	}
	atomic.AddInt64(&iostat.rx, int64(n))
	return n, err
}

type tx struct{ io.Writer }

func (w tx) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	atomic.AddInt64(&iostat.tx, int64(n))
	return n, err
}

type rxlim struct {
	lim int
	rx
}

func (r *rxlim) Read(p []byte) (n int, err error) {
	rx := int(atomic.LoadInt64(&iostat.rx))
	dur := int(time.Since(procstart) / (100 * time.Millisecond))
	bpms := 0
	if dur > 0 {
		bpms = rx / dur
	}
	if bpms*10 > r.lim {
		s := time.Duration((float64(bpms*10)/float64(r.lim) - 1) * float64(time.Second))
		if s >= 100*time.Millisecond {
			quantum()
			time.Sleep(s)
		}
	}
	return r.rx.Read(p)
}

// quantum releases the thread and prevents spinning in a loop
// it sleeps for double the actual quantum on linux, which is 2*100ms
func quantum() {
	if !*spin {
		time.Sleep(200 * time.Millisecond)
	}
}
