package storage

import (
	"fmt"
	"os"
	"path"
)

type Storage interface {
	OpenWriter(name string) (Writer, error)
}

type DiskStorage struct {
	dir string
}

func NewDiskStorage(dir string) *DiskStorage {
	os.MkdirAll(dir, 0644)
	fmt.Println(dir)
	return &DiskStorage{dir: dir}
}

func (ds *DiskStorage) OpenWriter(name string) (Writer, error) {
	return os.OpenFile(path.Join(ds.dir, name), os.O_CREATE|os.O_RDWR|os.O_APPEND,0644)
}