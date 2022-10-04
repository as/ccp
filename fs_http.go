package main

import (
	"context"
	"fmt"
	"io"
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
	//curl -v -H 'Range: bytes=0-0'  http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4
	return []Info{{URL: &u}}, nil
}

func (f HTTP) Open(file string) (io.ReadCloser, error) {
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
