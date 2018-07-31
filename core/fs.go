package core

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type FileRecord struct {
	Sha1      string
	MdbID     int64
	Size      int64
	ModTime   time.Time
	LocalCopy bool
}

func (r *FileRecord) FromLine(line string) error {
	s := strings.Split(line, ",")
	if len(s) != 4 {
		return errors.Errorf("Wrong number of fields %d", len(s))
	}

	if len(s[1]) != 42 {
		return errors.Errorf("bad checksum")
	}
	sha1 := s[1][1:41]

	size, err := strconv.ParseInt(s[2], 10, 64)
	if err != nil {
		return errors.Errorf("bad size: %s", err.Error())
	}

	unixTs, err := strconv.ParseInt(s[3][:len(s[3])-1], 10, 64)
	if err != nil {
		return errors.Errorf("bad modification time: %s", err.Error())
	}

	r.Sha1 = sha1
	r.Size = size
	r.ModTime = time.Unix(unixTs, 0)
	r.LocalCopy = true

	return nil
}

func (r *FileRecord) ToLine(path string) string {
	return fmt.Sprintf("[\"%s\",\"%s\",%d,%d]", path, r.Sha1, r.Size, r.ModTime.Unix())
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

func (fs *Sha1FS) ReadIndex() (map[string]*FileRecord, error) {
	f, err := os.Open(filepath.Join(fs.Root, "index"))
	if err != nil {
		return nil, errors.Wrap(err, "os.Open")
	}
	defer f.Close()

	i := 0
	idx := make(map[string]*FileRecord)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		i++
		line := strings.TrimSpace(scanner.Text())
		if line[0] == '#' {
			continue
		}

		r := new(FileRecord)
		if err := r.FromLine(line); err != nil {
			log.Printf("Sha1FS.ReadIndex: Bad line %d: %s\n", i, err.Error())
			continue
		}
		idx[r.Sha1] = r
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "scanner.Err()")
	}

	log.Printf("Sha1FS.ReadIndex: %d files in index\n", len(idx))

	return idx, nil
}

func (fs *Sha1FS) WriteIndex(idx map[string]*FileRecord) error {
	f, err := os.Create(filepath.Join(fs.Root, "index"))
	if err != nil {
		return errors.Wrap(err, "os.Create")
	}
	defer f.Close()

	for _, v := range idx {
		f.WriteString(v.ToLine(fs.Path(v.Sha1)))
		f.WriteString("\n")
	}

	return nil
}

func (fs *Sha1FS) ScanReap() error {
	// read current index
	idx, err := fs.ReadIndex()
	if err != nil {
		return errors.Wrap(err, "fs.ReadIndex")
	}

	// reset localCopy (we're about to validate it really is in local storage)
	for _, v := range idx {
		v.LocalCopy = false
	}

	// walk all files
	noChange := 0
	added := 0
	updated := 0
	removed := 0
	ghosts := 0
	err = filepath.Walk(fs.Root, func(path string, info os.FileInfo, err error) error {
		// skip directories and our own index file
		if info.IsDir() || info.Name() == "index" {
			return nil
		}

		// process file
		sha1 := info.Name()
		size := info.Size()
		modTime := info.ModTime().Unix()
		if r, ok := idx[sha1]; ok {
			r.LocalCopy = true

			if size == r.Size && modTime == r.ModTime.Unix() {
				noChange++
			} else {
				// File has changed from previous scan.
				// We compute it's checksum and compare.
				// If it matches the file's name we update the index else we drop it.

				cs, err := Sha1Sum(path)
				if err != nil {
					log.Printf("[ERROR] Sha1FS.ScanReap: compute modified file checksum %s: %s\n", path, err.Error())
					return nil
				}

				if sha1 == cs {
					idx[sha1].Size = size
					idx[sha1].ModTime = time.Unix(modTime, 0)
					updated++
				} else {
					delete(idx, sha1)
					log.Printf("Sha1FS.ScanReap: modified file bad checksum, removing physical file: %s\n", path)
					if err := os.Remove(path); err != nil {
						log.Printf("[ERROR] Sha1FS.ScanReap: remove bad file %s: %s\n", path, err.Error())
					}
					removed++
				}
			}
		} else {
			// first time we see this file
			cs, err := Sha1Sum(path)
			if err != nil {
				log.Printf("[ERROR] Sha1FS.ScanReap: compute new file checksum %s: %s\n", path, err.Error())
				return nil
			}

			idx[cs] = &FileRecord{
				Sha1:      cs,
				Size:      size,
				ModTime:   time.Unix(modTime, 0),
				LocalCopy: true,
			}
			added++
		}

		return nil
	})

	if err != nil {
		return errors.Wrap(err, "filepath.Walk")
	}

	// remove ghosts from index
	for k := range idx {
		if !idx[k].LocalCopy {
			ghosts++
			delete(idx, k)
		}
	}

	log.Printf("Sha1FS.ScanReap: %d no change, %d added, %d updated, %d removed, %d ghosts",
		noChange, added, updated, removed, ghosts)

	// write index to file
	if err := fs.WriteIndex(idx); err != nil {
		return errors.Wrap(err, "fs.WriteIndex")
	}

	return nil
}
