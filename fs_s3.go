package main

import (
	"context"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	s3	"github.com/aws/aws-sdk-go/service/s3"
)

type S3 struct {
	ctx context.Context
	c   *s3.S3
	err error
}

func (g *S3) ensure() bool {
	if g.ctx == nil {
		g.ctx = context.Background()
	}
	if g.c == nil {
		s, err := session.NewSession()
		g.err = err
		if err == nil {
			g.c= s3.New(s)
		}
	}
	return g.err == nil
}

func (g *S3) Open(file string) (io.ReadCloser, error) {
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

func (g *S3) Create(file string) (io.WriteCloser, error) {
	u := uri(file)
	pr, pw, err := os.Pipe()
	if err != nil{
		return nil, err
	}
	_, err= g.c.PutObject(&s3.PutObjectInput{
		Body:   pr,
		Bucket: &u.Host,
		Key:    &u.Path,
	})
	return pw, err
}

// sess.Copy(&aws.Config{Region: aws.String("us-east-2")})
