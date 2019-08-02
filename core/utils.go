package core

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

func Sha1Sum(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", errors.Wrap(err, "os.Open")
	}
	defer file.Close()

	hash := sha1.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", errors.Wrap(err, "io.Copy")
	}

	hashInBytes := hash.Sum(nil)[:20]
	return hex.EncodeToString(hashInBytes), nil
}

// mkdirp creates all missing parent directories for the destination file path.
func Mkdirp(path string) error {
	dir := filepath.Dir(path)
	if fi, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrapf(err, "check destination directory: %s", dir)
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return errors.Wrapf(err, "create destination directory: %s", dir)
		}
	} else if !fi.IsDir() {
		return errors.New("destination path is not a directory")
	}
	return nil
}
