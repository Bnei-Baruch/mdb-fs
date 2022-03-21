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

type Syncer interface {
	GetFS() *Sha1FS
	DoSync()
	AugmentMDBToIndex(idx map[string]*FileRecord) error
}

type SyncerImpl struct {
	FS          *Sha1FS
	quit        chan bool
	queue       *fetch.TaskQueue
	taskFactory *fetch.TaskFactory
	files       []*FileRecord
	pos         int
	wErrs       sync.Map
}

func NewSyncer() Syncer {
	return &SyncerImpl{
		FS:   NewSha1FS(),
		quit: make(chan bool),
	}
}

func (s *SyncerImpl) DoSync() {
	// initialize
	ticker := time.NewTicker(config.Config.SyncUpdateInterval)
	s.taskFactory = fetch.NewTaskFactory()
	s.queue = fetch.NewTaskQueue(config.Config.Fetchers)
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
				if err := s.FS.ScanReap(); err != nil {
					log.Printf("[ERROR] Syncer.FS.ScanReap %s\n", err.Error())
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

func (s *SyncerImpl) Close() {
	log.Println("[INFO] Syncer - signal quit channel")
	s.quit <- true

	log.Println("[INFO] Syncer - close work queue")
	Closer(s.queue).Close()
}

func (s *SyncerImpl) GetFS() *Sha1FS {
	return s.FS
}

func (s *SyncerImpl) OnTaskEnqueue(task fetch.Task) {
	// No-op
}

func (s *SyncerImpl) OnTaskRun(workerID int, task fetch.Task) {
	// No-op
}

func (s *SyncerImpl) OnTaskComplete(workerID int, task fetch.Task, resp interface{}, err error) {
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

func (s *SyncerImpl) enqueueNext() bool {
	if s.pos < len(s.files) {
		sha1 := s.files[s.pos].Sha1

		if s.FS.IsExistValid(sha1) {
			s.pos++
			return s.pos < len(s.files)
		}

		// skip files whom errored before (cleared on reload)
		if _, ok := s.wErrs.Load(sha1); ok {
			log.Printf("[INFO] Skipping file with error %s\n", sha1)
			s.pos++
			return s.pos < len(s.files)
		}

		task := s.taskFactory.Make(sha1, s.FS.Path(sha1))
		if err := s.queue.Enqueue(task, time.Second); err == nil {
			s.pos++
		}
	}

	return s.pos < len(s.files)
}

func (s *SyncerImpl) reload() error {
	log.Println("[INFO] Syncer.reload()")

	// read local index
	idx, err := s.FS.ReadIndex()
	if err != nil {
		return errors.Wrap(err, "[INFO] Syncer.FS.ReadIndex")
	}

	// augment index with missing files (in mdb not in local storage)
	err = s.AugmentMDBToIndex(idx)
	if err != nil {
		return errors.Wrap(err, "[INFO] Syncer.AugmentMDBToIndex")
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

func (s *SyncerImpl) AugmentMDBToIndex(idx map[string]*FileRecord) error {
	db, err := sql.Open("postgres", config.Config.MDBUrl)
	if err != nil {
		return errors.Wrap(err, "sql.Open")
	}
	defer db.Close()

	var query string
	if config.Config.MerkazAccess {
		query = `select distinct f.id, f.sha1
from files f
       inner join files_storages fs on f.id = fs.file_id
       inner join storages s on fs.storage_id = s.id and s.location = 'merkaz'
where f.sha1 is not null
  and f.removed_at is null
  and (f.published is true or f.type in ('image', 'text'));`
	} else {
		query = `select distinct f.id, f.sha1
from files f
	inner join files_storages fs on f.id = fs.file_id
where f.sha1 is not null
  and f.removed_at is null
  and f.published is true;`
	}

	rows, err := db.Query(query)
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

func (s *SyncerImpl) wErrsSize() int {
	size := 0
	s.wErrs.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}
