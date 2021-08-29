package main

import (
	"io"
	"os"
)

type OS struct {
}

func (f OS) Open(file string) (io.ReadCloser, error) {
	return os.Open(file)
}

func (f OS) Create(file string) (io.WriteCloser, error) {
	return os.Create(file)
}

func (f OS) Close() error { return nil }
