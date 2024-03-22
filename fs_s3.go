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

	"github.com/as/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	s3m "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3 struct {
	ctx context.Context
	s   *session.Session
	c   *s3.S3
	u   *s3m.Uploader
	ctr int64 // how many uploaders are uploading
	err error
}

type box struct {
	c *s3.S3
	u *s3m.Uploader
}

func (g *S3) ensure() bool {
	if g.ctx == nil {
		g.ctx = context.Background()
	}
	<-regionDetected
	if g.c == nil {
		s, err := session.NewSession()
		g.s = s
		g.err = err
		if err == nil {
			g.c = s3.New(s)
			g.u = s3m.NewUploader(s)
		}
	}
	return g.err == nil
}

func (g *S3) regionize(file string) (*s3.S3, *s3m.Uploader) {
	native := os.Getenv("AWS_REGION")
	r, _ := s3manager.GetBucketRegion(g.ctx, g.s, uri(file).Host, "")
	if r == "" {
		r = native
	}
	if r == native {
		return g.c, g.u
	}
	sess, _ := session.NewSession(&aws.Config{
		Region: &r,
	})
	if sess == nil {
		return g.c, g.u
	}
	log.Debug.F("using new region configuration: %q", r)
	return s3.New(sess), s3m.NewUploader(sess)
}

func (g *S3) List(dir string) (file []Info, err error) {
	if !g.ensure() {
		return nil, g.err
	}
	gc, _ := g.regionize(dir)
	u := uri(dir)
	dir = strings.TrimPrefix(u.Path, "/")

	var cursor *string // hare-brained sdk
Unroll:
	o, err := gc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:            &u.Host,
		Prefix:            &dir,
		ContinuationToken: cursor,
	})
	for _, v := range o.Contents {
		u := u
		u.Path = *v.Key
		file = append(file,
			Info{URL: &u, Size: int(*v.Size)},
		)
	}
	if v := o.NextContinuationToken; v != nil {
		cursor = v
		goto Unroll
	}

	return file, err
}

func (g *S3) Open(file string) (io.ReadCloser, error) {
	if !g.ensure() {
		return nil, g.err
	}
	gc, _ := g.regionize(file)
	u := uri(file)
	o, err := gc.GetObject(&s3.GetObjectInput{
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

var (
	extraGrants = strings.Split(os.Getenv("GRANTFULLCONTROL"), ",")
	s3acl       = "bucket-owner-full-control"
)

func (g *S3) ownerOf(bucket string) []string {
	r, err := g.c.GetBucketAcl(&s3.GetBucketAclInput{
		Bucket: &bucket,
	})
	if err != nil {
		return nil
	}
	return []string{*r.Owner.ID}
}

func (g *S3) uploadGrants(bucket string) string {
	grants, sep := "", ""
	for _, id := range append(g.ownerOf(bucket), extraGrants...) {
		if id == "" {
			continue
		}
		grants += sep + "id=" + id
		sep = ","
	}
	log.Info.F("grant full control to: %q", grants)
	return grants
}

func (g *S3) Create(file string) (io.WriteCloser, error) {
	if !g.ensure() {
		return nil, g.err
	}
	gc, gu := g.regionize(file)
	u := uri(file)
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	pipectl := &pipeline{
		wait:        make(chan error, 1),
		WriteCloser: pw,
	}

	grants := ""
	if !*test {
		grants = g.uploadGrants(u.Host)
	}
	acl := *acl
	if acl == "" {
		acl = s3acl
		if grants != "" {
			acl = ""
		}
	} else {
		grants = ""
	}

	go func() {
		atomic.AddInt64(&g.ctr, +1)
		defer atomic.AddInt64(&g.ctr, -1)
		br := bufio.NewReader(pr)
		data, _ := br.Peek(32)
		content := sniffContent(data)
		_, err = gu.Upload(&s3m.UploadInput{
			Body:        br,
			Bucket:      &u.Host,
			Key:         &u.Path,
			ContentType: &content,
		})
		if err == nil {
			_, err := gc.PutObjectAcl(&s3.PutObjectAclInput{
				Key:              &u.Path,
				Bucket:           &u.Host,
				ACL:              &acl,
				GrantFullControl: &grants,
			})
			if err != nil {
				log.Warn.F("s3: failed to grant full control: %s", err)
			}
		}
		pipectl.wait <- err
		if err != nil {
			pipectl.Close()
		}
	}()
	return pipectl, nil
}

func (g *S3) Close() error {
	for atomic.LoadInt64(&g.ctr) > 0 {
		log.Printf("s3: %d uploaders uploading", atomic.LoadInt64(&g.ctr))
		time.Sleep(time.Second)
	}
	return nil
}

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

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", idURL, nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		return "us-east-1"
	}
	json.NewDecoder(resp.Body).Decode(&ID)
	return ID.Region
}

const idURL = "http://169.254.169.254/latest/dynamic/instance-identity/document"
