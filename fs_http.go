package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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

func (g *HTTP) List(dir string) (file []Info, err error) {
	u := uri(dir)
	size, err := httpsize(dir)
	return []Info{{URL: &u, Size: size}}, err
	//curl -v -H 'Range: bytes=0-0'  http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4
}

func httpsize(dir string) (int, error) {
	r, err := http.NewRequest("GET", dir, nil)
	if err != nil {
		return 0, err
	}
	r.Header.Add("Range", "bytes=0-0")
	resp, err := http.DefaultClient.Do(r)
	if err != nil || resp.StatusCode/100 > 3 {
		if resp.StatusCode == 416 {
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
	req, _ := http.NewRequestWithContext(f.ctx, "GET", file, nil)
	resp, err := http.DefaultClient.Do(req)
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
