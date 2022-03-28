package importer

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/config"
	"github.com/Bnei-Baruch/mdb-fs/core"
)

const (
	ModeRename = "rename"
	ModeLink   = "link"
)

type FolderReshaper struct {
	syncer core.Syncer
	quit   chan bool
}

func NewFolderReshaper() *FolderReshaper {
	return &FolderReshaper{
		quit:   make(chan bool),
		syncer: core.NewSyncer(),
	}
}

// Reshape any folder structure to our own Sha1FS.
// This is intended to be some form of an import operation.
// We move (rename) files from some source folder to our own data folder.
// Iff the file is known to mdb
func (fr *FolderReshaper) Reshape(folder string, mode string) error {
	// read local index
	idx, err := fr.syncer.GetFS().ReadIndex()
	if err != nil {
		return errors.WithMessage(err, "FS.ReadIndex")
	}

	// augment index with missing files (in mdb not in local storage)
	err = fr.syncer.AugmentMDBToIndex(idx)
	if err != nil {
		return errors.WithMessage(err, "[INFO] Syncer.AugmentMDBToIndex")
	}

	csTasks := make(chan *core.ChecksumTask, 1024)
	csResults := make(chan *core.ChecksumTask)
	var wgWorkers sync.WaitGroup
	var wgCollector sync.WaitGroup
	var idxLock sync.RWMutex

	// counters incremented only in results collector
	exist := 0
	added := 0
	unknown := 0

	// ChecksumTask results collector
	go func(c chan *core.ChecksumTask) {
		for t := range c {
			if t.Err != nil {
				log.Printf("[ERROR] FolderReshaper.Reshape: compute file checksum %s: %s\n", t.Path, t.Err.Error())
				continue
			}

			idxLock.Lock()

			if r, ok := idx[t.Checksum]; ok {
				if r.LocalCopy {
					exist++
					if mode == ModeRename {
						log.Printf("[INFO] Remove existing file %s\n", t.Path)
						if err := os.Remove(t.Path); err != nil {
							log.Printf("[ERROR] FolderReshaper.Reshape: os.Remove %s: %s\n", t.Path, err)
						}
					}
				} else {
					dest := fr.syncer.GetFS().Path(t.Checksum)
					err := core.Mkdirp(dest)
					if err == nil {
						switch mode {
						case ModeLink:
							log.Printf("[INFO] Link new file %s\n", t.Path)
							err = os.Link(t.Path, dest)
						case ModeRename:
							log.Printf("[INFO] Rename new file %s\n", t.Path)
							err = os.Rename(t.Path, dest)
						default:
							panic(fmt.Sprintf("Unknown mode %s", mode))
						}
						if err == nil {
							added++
						} else {
							log.Printf("[ERROR] FolderReshaper.Reshape: os.Rename/Link %s => %s: %s\n", t.Path, dest, err)
						}
					} else {
						log.Printf("[ERROR] FolderReshaper.Reshape: Mkdirp %s: %s\n", dest, err)
					}
				}
			} else {
				unknown++
			}

			idxLock.Unlock()
		}
		wgCollector.Done()
	}(csResults)
	wgCollector.Add(1)

	// ChecksumTask workers
	for i := 0; i < config.Config.IndexWorkers; i++ {
		go func(id int, c chan *core.ChecksumTask, r chan<- *core.ChecksumTask) {
			for t := range c {
				t.Checksum, t.Err = core.Sha1Sum(t.Path)
				r <- t
			}
			wgWorkers.Done()
		}(i, csTasks, csResults)
		wgWorkers.Add(1)
	}

	// walk all files
	// create and enqueue tasks when necessary
	err = filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		// skip directories
		if info.IsDir() {
			return nil
		}

		// prepare task for file
		csTasks <- &core.ChecksumTask{
			Path:     path,
			FName:    info.Name(),
			FSize:    info.Size(),
			FModTime: info.ModTime().Unix(),
		}

		return nil
	})

	log.Println("[INFO] FolderReshaper.Reshape: done walking FS, closing tasks channel")
	close(csTasks)

	log.Println("[INFO] FolderReshaper.Reshape: waiting for workers to finish")
	wgWorkers.Wait()

	log.Println("[INFO] FolderReshaper.Reshape: closing results channel")
	close(csResults)

	log.Println("[INFO] FolderReshaper.Reshape: waiting for results collector to finish")
	wgCollector.Wait()

	if err != nil {
		return errors.Wrap(err, "filepath.Walk")
	}

	log.Printf("[INFO] FolderReshaper.Reshape: %d exist, %d added, %d unknown\n",
		exist, added, unknown)

	return nil
}
