package main

import (
	"context"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/as/log"
	"google.golang.org/api/iterator"
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

func (g *GS) List(dir string) (file []Info, err error) {
	if !g.ensure() {
		return nil, g.err
	}
	u := uri(dir)
	dir = strings.TrimPrefix(u.Path, "/")

	it := g.c.Bucket(u.Host).Objects(g.ctx, &storage.Query{Prefix: dir})
	for {
		attr, err := it.Next()
		if err == iterator.Done || err != nil {
			break
		}
		u := u
		u.Path = attr.Name
		file = append(file, Info{URL: &u, Size: int(attr.Size)})
	}
	return file, err
}

func (g *GS) Open(file string) (io.ReadCloser, error) {
	u := uri(file)
	u.Path = strings.TrimPrefix(u.Path, "/")
	log.Debug.Add("host", u.Host, "path", u.Path).Printf("open")
	if !g.ensure() {
		return nil, g.err
	}
	return g.c.Bucket(u.Host).Object(u.Path).NewReader(g.ctx)
}

func (g *GS) Create(file string) (io.WriteCloser, error) {
	if !g.ensure() {
		return nil, g.err
	}
	u := uri(file)
	u.Path = strings.TrimPrefix(u.Path, "/")
	log.Debug.Add("host", u.Host, "path", u.Path).Printf("create")
	return g.c.Bucket(u.Host).Object(u.Path).NewWriter(g.ctx), nil
}

func (f GS) Close() error {
	log.Debug.F("closed")
	return nil
}
