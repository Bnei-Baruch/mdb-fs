package core

import (
	"database/sql"
	"encoding/hex"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/cavaliercoder/grab"
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
	wErrs       sync.Map
}

func (s *Syncer) DoSync(cfg *config.Config) {
	s.cfg = cfg

	s.quit = make(chan bool)
	ticker := time.NewTicker(cfg.SyncUpdateInterval)

	// initialize Sha1 FS
	s.fs = NewSha1FS(cfg)

	// initialize fetchers
	s.taskFactory = fetch.NewTaskFactory(cfg)
	s.queue = fetch.NewTaskQueue(cfg)
	s.queue.AddListener(s)

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
			// no more items to process

			// update index if no download is in progress
			if s.queue.Size() == 0 {
				log.Println("[INFO] No more files to download, no jobs in queue. Scan & Reap index")
				if err := s.fs.ScanReap(); err != nil {
					log.Printf("[ERROR] Syncer.fs.ScanReap %s\n", err.Error())
				}
			}

			// blocking wait for either quit or reload
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
	log.Println("[INFO] Syncer - signal quit channel")
	s.quit <- true

	log.Println("[INFO] Syncer - close work queue")
	s.queue.Close()
}

func (s *Syncer) OnTaskEnqueue(task fetch.Task) {
	// No-op
}

func (s *Syncer) OnTaskRun(workerID int, task fetch.Task) {
	// No-op
}

func (s *Syncer) OnTaskComplete(workerID int, task fetch.Task, resp interface{}, err error) {
	ft := task.(*fetch.FetchTask)
	if err != nil {
		log.Printf("[ERROR] Worker %d: %s\n", workerID, err.Error())
		s.wErrs.Store(ft.Sha1, err)
	} else {
		gResp := resp.(*grab.Response)
		originUrl := ft.OriginUrl()
		if pUrl, err := url.Parse(originUrl); err == nil {
			originUrl = pUrl.Host
		}
		log.Printf("[INFO] Worker %d: Downloaded %s from %s in %d[s] at %.f[KBps]\n",
			workerID, ft.Sha1, originUrl, gResp.Duration()/time.Second, gResp.BytesPerSecond()/1024)
	}
}

func (s *Syncer) enqueueNext() bool {
	if s.pos < len(s.files) {
		sha1 := s.files[s.pos].Sha1

		if s.fs.IsExistValid(sha1) {
			s.pos++
			return s.pos < len(s.files)
		}

		// skip files whom errored before (cleared on reload)
		if _, ok := s.wErrs.Load(sha1); ok {
			log.Printf("[INFO] Skipping file with error %s\n", sha1)
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
	log.Println("[INFO] Syncer.reload()")

	// read local index
	idx, err := s.fs.ReadIndex()
	if err != nil {
		return errors.Wrap(err, "[INFO] Syncer.fs.ReadIndex")
	}

	// augment index with missing files (in mdb not in local storage)
	err = s.reloadMDB(idx)
	if err != nil {
		return errors.Wrap(err, "[INFO] Syncer.reloadMDB")
	}

	// determine files to fetch
	files := make([]*FileRecord, 0)
	for _, v := range idx {
		if v.MdbID > 0 && !v.LocalCopy {
			files = append(files, v)
		}
	}

	log.Printf("[INFO] %d files to fetch\n", len(files))
	s.files = files
	s.pos = 0

	log.Printf("[INFO] %d files with errors. Clearing...\n", s.wErrsSize())
	s.wErrs = sync.Map{}

	return nil
}

func (s *Syncer) reloadMDB(idx map[string]*FileRecord) error {
	db, err := sql.Open("postgres", s.cfg.MdbUrl)
	if err != nil {
		return errors.Wrap(err, "sql.Open")
	}
	defer db.Close()

	rows, err := db.Query(`select f.id, f.sha1
from files f
       inner join files_storages fs on f.id = fs.file_id
       inner join storages s on fs.storage_id = s.id and s.location = 'merkaz'
where f.sha1 is not null
  and f.removed_at is null
  and (f.published is true or f.type in ('image', 'text'));`)
	//rows, err := db.Query(`select id, sha1
	//from files
	//where sha1 is not null
	//  and removed_at is null
	//  and (published is true or type in ('image', 'text'))`)
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
		if r, ok := idx[sha1]; ok {
			r.MdbID = id
		} else {
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

func (s *Syncer) wErrsSize() int {
	size := 0
	s.wErrs.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}
