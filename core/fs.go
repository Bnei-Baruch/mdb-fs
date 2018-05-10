package core

import (
	"path/filepath"
)

type FileRecord struct {
	Sha1      string
	MdbID     int64
	LocalCopy bool
}

type Sha1FS struct {
	Root string
}

func NewSha1FS(root string) *Sha1FS {
	fs := new(Sha1FS)
	fs.Root = root
	return fs
}

func (fs *Sha1FS) Path(checksum string) string {
	return filepath.Join(fs.Root, checksum[0:1], checksum[1:2], checksum[2:3], checksum)
}

func (fs *Sha1FS) IsExistValid(checksum string) bool {
	if cs, err := Sha1Sum(fs.Path(checksum)); err == nil {
		return cs == checksum
	}

	return false
}
