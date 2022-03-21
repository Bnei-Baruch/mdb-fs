package core

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/config"
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
	Root            string
	ScanReapWorkers int
}

func NewSha1FS() *Sha1FS {
	fs := new(Sha1FS)
	fs.Root = config.Config.RootDir
	fs.ScanReapWorkers = config.Config.IndexWorkers
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

type ChecksumTask struct {
	Path     string
	Checksum string
	Err      error
	FName    string
	FSize    int64
	FModTime int64
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

	csTasks := make(chan *ChecksumTask, 1024)
	csResults := make(chan *ChecksumTask)
	var wgWorkers sync.WaitGroup
	var wgCollector sync.WaitGroup
	var idxLock sync.RWMutex

	// counters incremented only in results collector
	noChange := 0
	added := 0
	updated := 0
	removed := 0
	ghosts := 0

	// ChecksumTask results collector
	go func(c chan *ChecksumTask) {
		for t := range c {
			if t.Err != nil {
				log.Printf("[ERROR] Sha1FS.ScanReap: compute file checksum %s: %s\n", t.Path, t.Err.Error())
				continue
			}

			idxLock.Lock()

			// if computed checksum matches the file name we update the index
			// else we drop it

			if t.FName == t.Checksum {
				if r, ok := idx[t.FName]; ok {
					r.Size = t.FSize
					r.ModTime = time.Unix(t.FModTime, 0)
					updated++
				} else {
					idx[t.Checksum] = &FileRecord{
						Sha1:      t.Checksum,
						Size:      t.FSize,
						ModTime:   time.Unix(t.FModTime, 0),
						LocalCopy: true,
					}
					added++
				}
			} else {
				delete(idx, t.FName)
				log.Printf("Sha1FS.ScanReap: modified file bad checksum, removing physical file: %s\n", t.Path)
				if err := os.Remove(t.Path); err != nil {
					log.Printf("[ERROR] Sha1FS.ScanReap: remove bad file %s: %s\n", t.Path, err.Error())
				}
				removed++
			}

			idxLock.Unlock()
		}
		wgCollector.Done()
	}(csResults)
	wgCollector.Add(1)

	// ChecksumTask workers
	for i := 0; i < fs.ScanReapWorkers; i++ {
		go func(id int, c chan *ChecksumTask, r chan<- *ChecksumTask) {
			for t := range c {
				t.Checksum, t.Err = Sha1Sum(t.Path)
				r <- t
			}
			wgWorkers.Done()
		}(i, csTasks, csResults)
		wgWorkers.Add(1)
	}

	// walk all files
	// create and enqueue tasks when necessary
	err = filepath.Walk(fs.Root, func(path string, info os.FileInfo, err error) error {
		// skip directories and our own index file
		if info.IsDir() || info.Name() == "index" {
			return nil
		}

		// prepare task for file
		csTask := &ChecksumTask{
			Path:     path,
			FName:    info.Name(),
			FSize:    info.Size(),
			FModTime: info.ModTime().Unix(),
		}

		idxLock.RLock()
		r, ok := idx[csTask.FName]
		idxLock.RUnlock()

		if ok {
			r.LocalCopy = true

			// no change
			if csTask.FSize == r.Size && csTask.FModTime == r.ModTime.Unix() {
				noChange++
				return nil
			}
		}

		csTasks <- csTask

		return nil
	})

	log.Println("Sha1FS.ScanReap: done walking FS, closing tasks channel")
	close(csTasks)

	log.Println("Sha1FS.ScanReap: waiting for workers to finish")
	wgWorkers.Wait()

	log.Println("Sha1FS.ScanReap: closing results channel")
	close(csResults)

	log.Println("Sha1FS.ScanReap: waiting for results collector to finish")
	wgCollector.Wait()

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

	log.Printf("Sha1FS.ScanReap: %d no change, %d added, %d updated, %d removed, %d ghosts\n",
		noChange, added, updated, removed, ghosts)

	// write index to file
	if err := fs.WriteIndex(idx); err != nil {
		return errors.Wrap(err, "fs.WriteIndex")
	}

	return nil
}
