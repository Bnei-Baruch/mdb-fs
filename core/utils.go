package core

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
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

// Mkdirp creates all missing parent directories for the destination file path.
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

// HumanizeBytes format the given number of bytes for easier human reading in SI units.
func HumanizeBytes(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}
