package main

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type GS struct {
	ctx context.Context
	c   *storage.Client
	err error
}

func (g *GS) ensure() bool {
	if g.ctx == nil {
		g.ctx = context.Background()
	}
	if g.c == nil {
		g.c, g.err = storage.NewClient(g.ctx)
	}
	return g.err == nil
}

func (g *GS) Open(file string) (io.ReadCloser, error) {
	if !g.ensure() {
		return nil, g.err
	}
	u := uri(file)
	return g.c.Bucket(u.Host).Object(u.Path).NewReader(g.ctx)
}

func (g *GS) Create(file string) (io.WriteCloser, error) {
	if !g.ensure() {
		return nil, g.err
	}
	u := uri(file)
	return g.c.Bucket(u.Host).Object(u.Path).NewWriter(g.ctx), nil
}
