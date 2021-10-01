package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	//	"path"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	s3m "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3 struct {
	ctx context.Context
	c   *s3.S3
	u   *s3m.Uploader
	ctr int64 // how many uploaders are uploading
	err error
}

func (g *S3) ensure() bool {
	if g.ctx == nil {
		g.ctx = context.Background()
	}
	<-regionDetected
	if g.c == nil {
		s, err := session.NewSession()
		g.err = err
		if err == nil {
			g.c = s3.New(s)
			g.u = s3m.NewUploader(s)
		}
	}
	return g.err == nil
}

func (g *S3) List(dir string) (file []Info, err error) {
	if !g.ensure() {
		return nil, g.err
	}
	u := uri(dir)
	dir = strings.TrimPrefix(u.Path, "/")
	o, err := g.c.ListObjects(&s3.ListObjectsInput{
		Bucket: &u.Host,
		Prefix: &dir,
	})
	for _, v := range o.Contents {
		u := u
		u.Path = *v.Key
		file = append(file,
			Info{URL: &u, Size: int(*v.Size)},
		)
	}
	return file, err
}

func (g *S3) Open(file string) (io.ReadCloser, error) {
	if !g.ensure() {
		return nil, g.err
	}
	u := uri(file)
	o, err := g.c.GetObject(&s3.GetObjectInput{
		Bucket: &u.Host,
		Key:    &u.Path,
	})
	if err != nil {
		return nil, err
	}
	return o.Body, nil
}

type pipeline struct {
	wait chan error
	err  error
	io.WriteCloser
}

func (p *pipeline) Write(b []byte) (int, error) {
	n, err := p.WriteCloser.Write(b)
	if err != nil {
		if e := p.Err(); e != nil {
			err = e
		}
	}
	return n, err
}

func (p *pipeline) Close() error {
	p.WriteCloser.Close()
	select {
	case err, first := <-p.wait:
		if first {
			p.err = err
			close(p.wait)
		}
	}
	return p.err
}

func (p *pipeline) Err() error {
	select {
	case err, first := <-p.wait:
		if first {
			p.err = err
			close(p.wait)
		}
	default:
	}
	return p.err
}

var s3acl = "bucket-owner-full-control"

func (g *S3) Create(file string) (io.WriteCloser, error) {
	if !g.ensure() {
		return nil, g.err
	}
	u := uri(file)
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	pipectl := &pipeline{
		wait:        make(chan error, 1),
		WriteCloser: pw,
	}
	go func() {
		atomic.AddInt64(&g.ctr, +1)
		defer atomic.AddInt64(&g.ctr, -1)
		_, err = g.u.Upload(&s3m.UploadInput{
			Body:   bufio.NewReader(pr),
			Bucket: &u.Host,
			Key:    &u.Path,
			ACL:    &s3acl,
		})
		pipectl.wait <- err
		if err != nil {
			pipectl.Close()
		}
	}()
	return pipectl, nil
}

func (g *S3) Close() error {
	for atomic.LoadInt64(&g.ctr) > 0 {
		println("s3", atomic.LoadInt64(&g.ctr), "uploaders uploading")
		time.Sleep(time.Second)
	}
	return nil
}

// sess.Copy(&aws.Config{Region: aws.String("us-east-2")})

var regionDetected = make(chan bool)

func init() {
	go func() {
		os.Setenv("AWS_REGION", awsRegion())
		close(regionDetected)
	}()
}

func awsRegion() string {
	r := os.Getenv("AWS_REGION")
	if r != "" {
		return r
	}

	ID := struct {
		Region string
	}{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", idURL, nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		return ""
	}
	json.NewDecoder(resp.Body).Decode(&ID)
	return ID.Region
}

const idURL = "http://169.254.169.254/latest/dynamic/instance-identity/document"
