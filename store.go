package main

import (
	"errors"
	"io"
	"net/url"
)

type StoreFile struct {
	Name string
	Size int
}

type Store interface {
	Upload(name string, body io.Reader) error
	Download(name string) (io.ReadCloser, error)
	List() ([]*StoreFile, error)
}

func NewStore(u string) (Store, error) {
	su, err := url.Parse(u)
	if err != nil {
		return nil, err
	} else if su.Scheme == "s3" {
		return newS3Store(su)
	}
	return nil, errors.New("unknown scheme")
}

type Upload struct {
	Name string
	Body io.Reader
}

func (a Agent) Uploader() error {
	for {
		select {
		case <-a.exitC:
			return nil
		case u := <-a.uploadC:
			err := a.store.Upload(u.Name, u.Body)
			if err != nil {
				// todo: retry
				return err
			}
		}
	}
}
