package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/as/log"
)

type HTTP struct {
	ctx context.Context
}

func (g *HTTP) ensure() bool {
	if g.ctx == nil {
		g.ctx = context.Background()
	}
	return true
}

func (g *HTTP) Delete(dir string) (err error) {
	return errors.New("not yet implemented")
}

func (g *HTTP) List(dir string) (file []Info, err error) {
	u := uri(dir)
	size, err := httpsize(dir)
	return []Info{{URL: &u, Size: size}}, err
}

var cache = sync.Map{}

func newHTTPRequest(verb, path string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(verb, path, body)
	if err != nil {
		return nil, err
	}
	if *header != "" {
		k, v, _ := strings.Cut(*header, ":")
		r.Header.Add(k, v)
	}
	if *agent != "" {
		r.Header.Add("User-Agent", *agent)
	}
	return r, nil
}

func httpsize(dir string) (size int, err error) {
	v, _ := cache.Load(dir)
	if v, _ := v.(int); v != 0 {
		return v, nil
	}
	defer func() {
		if err == nil {
			cache.Store(dir, size)
		}
	}()
	r, err := newHTTPRequest("GET", dir, nil)
	if err != nil {
		return 0, err
	}
	r.Header.Add("Range", "bytes=0-0")
	resp, err := http.DefaultClient.Do(r)
	if *debug {
		logopen("httpsize", dir, resp, err)
	}
	if err != nil || resp.StatusCode/100 > 3 {
		if err == nil && resp.StatusCode == 416 {
			return 0, nil
		}
		if err == nil {
			return 0, fmt.Errorf("http: %s", resp.Status)
		}
		return 0, err
	}
	go func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	a, b, size := 0, 0, 0
	_, err = fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &a, &b, &size)
	return size, err
}

func (f HTTP) Open(file string) (io.ReadCloser, error) {
	if *slow {
		return f.open(file)
	}
	r, err := f.fastopen(file)
	if err != nil {
		return f.open(file)
	}
	return r, err
}

func (f HTTP) open(file string) (io.ReadCloser, error) {
	f.ensure()
	req, err := newHTTPRequest("GET", file, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(f.ctx)
	if *seek != 0 || *count != 0 {
		if *count == 0 {
			req.Header.Add("Range", fmt.Sprintf("bytes=%d-", *seek))
		} else {
			req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", *seek, *seek+*count-1))
		}
	}
	resp, err := http.DefaultClient.Do(req)

	if *debug {
		logopen("slowopen", file, resp, err)
	}
	if err != nil || resp.StatusCode >= 400 {
		if err == nil {
			err = fmt.Errorf("status: %v", resp.StatusCode)
		}
		// NOTE(as): bug here with connection reuse
		// if the body isn't read+closed by callee
		return nil, err
	}
	return resp.Body, err
}

func (f HTTP) Create(file string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("http: create: not implemented yet")
}

func (f HTTP) Close() error { return nil }

func logopen(caller string, file string, resp *http.Response, err error) {
	h := []string{}
	for k := range resp.Header {
		h = append(h, k)
		h = append(h, resp.Header.Get(k))
	}
	log.Debug.Add("action", "open", "func", caller, "file", file, "status", resp.StatusCode, "error", err, "headers", h).Printf("")
}
