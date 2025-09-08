package fs

import (
	"io"
	"os"
	"path"
)

type File interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.WriterAt
	io.Seeker
	io.Closer
	Sync() error
}

type FileSystem interface {
	MkdirAll(path string, perm os.FileMode) error
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	ReadDir(dirname string) ([]os.DirEntry, error)
}

type OSFileSystem struct{
	Dir string
}

func (fs OSFileSystem) MkdirAll(name string, perm os.FileMode) error {
	return os.MkdirAll(path.Join(fs.Dir, name), perm)
}

func (fs OSFileSystem) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(path.Join(fs.Dir, name), flag, perm)
}

func (fs OSFileSystem) ReadDir(dirname string) ([]os.DirEntry, error) {
	return os.ReadDir(dirname)
}
