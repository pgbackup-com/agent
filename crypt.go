package main

import (
	"bytes"
	"compress/gzip"
	"crypto/cipher"
	"crypto/sha256"
	"io"
)

type cryptStore struct {
	Store
	Aes cipher.Block
}

func (s cryptStore) stream(name string) cipher.Stream {
	iv := sha256.Sum256(([]byte)(name))
	return cipher.NewCTR(s.Aes, iv[:16])
}

func (s cryptStore) Upload(name string, body io.Reader) error {
	buf := &bytes.Buffer{}
	var w io.Writer
	w = &cipher.StreamWriter{W: buf, S: s.stream(name)}
	w = gzip.NewWriter(w)
	_, err := io.Copy(w, body)
	if err != nil {
		return err
	}
	w.(*gzip.Writer).Close()
	return s.Store.Upload(name, buf)
}

func (s cryptStore) Download(name string) (io.ReadCloser, error) {
	r, err := s.Store.Download(name)
	if err != nil {
		return nil, err
	}

	var r0 io.Reader
	r0 = &cipher.StreamReader{R: r, S: s.stream(name)}
	r0, err = gzip.NewReader(r0)
	if err != nil {
		r.Close()
		return nil, err
	}

	r = &otherCloser{
		Reader: r0,
		Closer: r,
	}
	return r, nil
}

type otherCloser struct {
	io.Reader
	Closer io.Closer
}

func (oc otherCloser) Close() error {
	return oc.Closer.Close()
}
