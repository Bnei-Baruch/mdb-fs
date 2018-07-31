package core

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/config"
	"github.com/Bnei-Baruch/mdb-fs/fetch"
)

type Syncer struct {
	cfg         *config.Config
	fs          *Sha1FS
	quit        chan bool
	queue       *fetch.TaskQueue
	taskFactory *fetch.TaskFactory
	files       []*FileRecord
	pos         int
	reloads     int
}

func (s *Syncer) DoSync(cfg *config.Config) {
	s.cfg = cfg

	s.quit = make(chan bool)
	ticker := time.NewTicker(cfg.SyncUpdateInterval)

	// initialize Sha1 FS
	s.fs = NewSha1FS(cfg.RootDir)

	// initialize fetchers
	s.taskFactory = fetch.NewTaskFactory(cfg)
	s.queue = fetch.NewTaskQueue(cfg)

	// first time reload
	if err := s.reload(); err != nil {
		log.Printf("[ERROR] Syncer.reload %s\n", err.Error())
	}

	// batch process until quit or refresh
	for {
		if s.enqueueNext() {
			// we have more to enqueue so we use a non blocking select
			// with default case to continue our loop
			select {
			case <-s.quit:
				return
			case <-ticker.C:
				if err := s.reload(); err != nil {
					log.Printf("[ERROR] Syncer.reload %s\n", err.Error())
				}
			default:
				continue
			}
		} else {
			// no more items to process, blocking wait for either quit or reload
			select {
			case <-s.quit:
				return
			case <-ticker.C:
				if err := s.reload(); err != nil {
					log.Printf("[ERROR] Syncer.reload %s\n", err.Error())
				}
			}
		}
	}
}

func (s *Syncer) Close() {
	log.Println("Syncer - signal quit channel")
	s.quit <- true

	log.Println("Syncer - close work queue")
	s.queue.Close()
}

func (s *Syncer) enqueueNext() bool {
	if s.pos < len(s.files) {
		sha1 := s.files[s.pos].Sha1

		if s.fs.IsExistValid(sha1) {
			s.pos++
			return s.pos < len(s.files)
		}

		task := s.taskFactory.Make(sha1, s.fs.Path(sha1))
		if err := s.queue.Enqueue(task, time.Second); err == nil {
			s.pos++
		}
	}

	return s.pos < len(s.files)
}

func (s *Syncer) reload() error {
	log.Println("Syncer.reload()")
	s.reloads++

	// read local index
	idx, err := s.reloadLocal()
	if err != nil {
		return errors.Wrap(err, "Syncer.reloadLocal")
	}

	// augment index with missing files (in mdb not in local storage)
	err = s.reloadMDB(idx)
	if err != nil {
		return errors.Wrap(err, "Syncer.reloadMDB")
	}

	// determine files to fetch
	files := make([]*FileRecord, 0)
	for _, v := range idx {
		if v.MdbID > 0 && !v.LocalCopy {
			files = append(files, v)
		}
	}

	log.Printf("%d files to fetch\n", len(files))

	s.files = files
	s.pos = 0

	return nil
}

func (s *Syncer) reloadLocal() (map[string]*FileRecord, error) {
	f, err := os.Open(filepath.Join(s.cfg.RootDir, "index"))
	if err != nil {
		return nil, errors.Wrap(err, "os.Open")
	}

	i := 0
	idx := make(map[string]*FileRecord)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		i++
		line := strings.TrimSpace(scanner.Text())
		if line[0] == '#' {
			continue
		}

		s := strings.Split(line, ",")
		if len(s) != 4 || len(s[1]) != 42 {
			log.Printf("Syncer.reloadLocal: Bad line %d\n", i)
			continue
		}

		sha1 := s[1][1:41]
		idx[sha1] = &FileRecord{
			Sha1:      sha1,
			LocalCopy: true,
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "scanner.Err()")
	}

	log.Printf("Syncer.reloadLocal: %d files in local FS\n", len(idx))

	return idx, nil
}

func (s *Syncer) reloadMDB(idx map[string]*FileRecord) error {
	db, err := sql.Open("postgres", s.cfg.MdbUrl)
	if err != nil {
		return errors.Wrap(err, "sql.Open")
	}
	defer db.Close()

	rows, err := db.Query("select id, sha1 from files where sha1 is not null and published is true order by created_at desc limit 20")
	if err != nil {
		return errors.Wrap(err, "db.Query")
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var b []byte
		err = rows.Scan(&id, &b)
		if err != nil {
			return errors.Wrap(err, "rows.Scan")
		}

		sha1 := hex.EncodeToString(b)
		if _, ok := idx[sha1]; !ok {
			idx[sha1] = &FileRecord{
				Sha1:      sha1,
				MdbID:     id,
				LocalCopy: false,
			}
		}
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "rows.Err()")
	}

	return nil
}