package storage

import "io"

type Writer interface {
	io.WriteCloser
	Sync() error
}