package main

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type OS struct {
}

func (f OS) Delete(dir string) (err error) {
	return os.Remove(dir)
}

func (f OS) List(dir string) (file []Info, err error) {
	dir = localize(dir)
	u := uri(dir)
	if dir == "-" {
		return []Info{{URL: &u}}, nil
	}
	return file, filepath.Walk(dir, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			u := u
			u.Path = p
			file = append(file, Info{URL: &u, Size: int(info.Size())})
		}
		return nil
	})
}

func (f OS) Open(file string) (io.ReadCloser, error) {
	file = localize(file)
	if file == "-" {
		return os.Stdin, nil
	}
	return os.Open(file)
}

func (f OS) Create(file string) (io.WriteCloser, error) {
	file = localize(file)
	if file == "-" {
		// Invariant: users with files named "-" will not
		// ruin it for the rest of us.
		return os.Stdout, nil
	}
	w, err := os.Create(file)
	if errors.Is(err, os.ErrNotExist) {
		os.MkdirAll(filepath.Dir(file), 0777)
		w, err = os.Create(file)
	}
	return w, err
}

func (f OS) Close() error { return nil }

func localize(file string) string {
	if strings.HasPrefix(file, "file://") { // rooted prefix
		return strings.TrimPrefix(file, "file://")
	}
	return file
}
