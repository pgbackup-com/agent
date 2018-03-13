package main

import (
	"bytes"
	"io"
	"log"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Store struct {
	S3     *s3.S3
	Bucket string
	Prefix string
}

func newS3Store(u *url.URL) (*s3Store, error) {
	awsKey := u.User.Username()
	awsSecret, _ := u.User.Password()
	awsSes, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(awsKey, awsSecret, ""),
		Region:      aws.String("eu-west-1"),
	})
	if err != nil {
		return nil, err
	}
	pf := u.Path[1:]
	if !strings.HasSuffix(pf, "/") {
		pf += "/"
	}
	return &s3Store{
		S3:     s3.New(awsSes),
		Bucket: u.Host,
		Prefix: pf,
	}, nil
}

func (s s3Store) Upload(name string, body io.Reader) error {
	rs, _ := body.(io.ReadSeeker)
	if rs == nil {
		buf := &bytes.Buffer{}
		_, err := io.Copy(buf, body)
		if err != nil {
			return err
		}
		rs = bytes.NewReader(buf.Bytes())
	}
	_, err := s.S3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.Prefix + name),
		Body:   rs,
	})
	log.Print("s3: upload ", name)
	return err
}

func (s s3Store) Download(name string) (io.ReadCloser, error) {
	o, err := s.S3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.Prefix + name),
	})
	if err != nil {
		return nil, err //fmt.Errorf("noSuchFile=%s%s", a.AwsPrefix, name)
	}
	log.Print("s3: download ", name)
	return o.Body, nil
}

func (s s3Store) List() ([]*StoreFile, error) {
	log.Print("list bucket=", s.Bucket, " prefix=", s.Prefix)
	ls, err := s.S3.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(s.Prefix),
	})
	if err != nil {
		return nil, err
	}

	var fs []*StoreFile
	for _, o := range ls.Contents {
		k := *(o.Key)
		if strings.HasPrefix(k, s.Prefix) {
			k = k[len(s.Prefix):]
			fs = append(fs, &StoreFile{Name: k, Size: int(*(o.Size))})
		}
	}

	// todo: paginate, (sort?)

	return fs, nil
}
