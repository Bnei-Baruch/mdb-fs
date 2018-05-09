package fetch

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/config"
)

type TaskQueue struct {
	jobs   chan Task
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewTaskQueue(cfg *config.Config) *TaskQueue {
	q := new(TaskQueue)
	q.jobs = make(chan Task)

	var ctx context.Context
	ctx, q.cancel = context.WithCancel(context.Background())

	for i := 0; i < cfg.Fetchers; i++ {
		// Start worker here.
		go func(id int) {
			// process jobs all my life
			for job := range q.jobs {
				if err := job.Do(ctx); err != nil {
					log.Printf("[ERROR]: %s\n", err.Error())
				}
			}

			// tell the waiting group I'm done
			q.wg.Done()
		}(i)

		// Add the worker to the waiting group.
		q.wg.Add(1)
	}

	return q
}

func (q *TaskQueue) Close() {
	log.Println("TaskQueue - close jobs channel.")
	close(q.jobs)

	log.Println("TaskQueue - cancel workers context.")
	q.cancel()

	log.Println("TaskQueue - wait for workers to finish.")
	if !WaitTimeout(&q.wg, 5*time.Second) {
		log.Println("TaskQueue - WaitGroup timeout.")
	}
}

func (q *TaskQueue) Enqueue(task Task, timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return errors.New("TIMEOUT")
	case q.jobs <- task:
		return nil
	}
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
