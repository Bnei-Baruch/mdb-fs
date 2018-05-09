package fetch

import (
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/config"
)

type WorkQueue interface {
	Init(cfg *config.Config)
	Close()
	Enqueue(WorkTask, time.Duration) error
}

type WorkTask interface {
	Do() error
}

type TaskQueue struct {
	jobs chan WorkTask
	wg   sync.WaitGroup
}

func (q *TaskQueue) Init(cfg *config.Config) {
	q.jobs = make(chan WorkTask)

	for i := 0; i < cfg.Fetchers; i++ {
		// Start worker here.
		go func(id int) {
			// process jobs all my life
			for job := range q.jobs {
				log.Printf("worker %d start %v\n", id, job)
				if err := job.Do(); err != nil {
					log.Printf("[ERROR]: worker %d finished %v with %s \n", id, job, err.Error())
				} else {
					log.Printf("worker %d finish %v\n", id, job)
				}
			}

			// tell the waiting group I'm done
			q.wg.Done()
		}(i)

		// Add the worker to the waiting group.
		q.wg.Add(1)
	}
}

func (q *TaskQueue) Close() {
	log.Println("TaskQueue - close jobs channel.")
	close(q.jobs)

	log.Println("TaskQueue - wait for workers to finish.")
	if !WaitTimeout(&q.wg, 5*time.Second) {
		log.Println("TaskQueue - WaitGroup timeout.")
	}
}

func (q *TaskQueue) Enqueue(task WorkTask, timeout time.Duration) error {
	//q.jobs <- task

	//for {
	select {
	case <-time.After(timeout):
		return errors.New("TIMEOUT")
	case q.jobs <- task:
		return nil
	}
	//}
}

// WaitTimeout does a Wait on a sync.WaitGroup object but with a specified
// timeout. Returns true if the wait completed without timing out, false
// otherwise.
func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}
