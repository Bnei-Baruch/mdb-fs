package core

import (
	"database/sql"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/cavaliergopher/grab/v3"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/common"
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
	ticker := time.NewTicker(common.Config.SyncUpdateInterval)
	s.taskFactory = fetch.NewTaskFactory()
	s.queue = fetch.NewTaskQueue(common.Config.Fetchers)
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
	var bytesToFetch int64
	for _, v := range idx {
		if v.MdbID > 0 && !v.LocalCopy {
			files = append(files, v)
			bytesToFetch += v.MdbSize
		}
	}

	log.Printf("[INFO] %d files to fetch, %s.\n", len(files), HumanizeBytes(bytesToFetch))
	s.files = files
	s.pos = 0

	log.Printf("[INFO] %d files with errors. Clearing...\n", s.wErrsSize())
	s.wErrs = sync.Map{}

	return nil
}

func (s *SyncerImpl) AugmentMDBToIndex(idx map[string]*FileRecord) error {
	db, err := sql.Open("postgres", common.Config.MDBUrl)
	if err != nil {
		return errors.Wrap(err, "sql.Open")
	}
	defer db.Close()

	strategy, err := MakeMDBStrategy(common.Config.MDBStrategy, common.Config.MDBStrategyParams)
	if err != nil {
		log.Fatalf("MakeMDBStrategy: %s", err.Error())
	}

	if err = strategy.Augment(db, idx); err != nil {
		return errors.Wrap(err, "strategy.Augment")
	}

	var fCount, bCount int64
	for _, v := range idx {
		if v.MdbID > 0 {
			fCount++
			bCount += v.MdbSize
		}
	}
	log.Printf("[INFO] MDB strategy has %d files, %s.\n", fCount, HumanizeBytes(bCount))

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
