package fetch

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type TaskListener interface {
	OnTaskEnqueue(Task)
	OnTaskRun(int, Task)
	OnTaskComplete(int, Task, interface{}, error)
}

type TaskQueue struct {
	jobs      chan Task
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	listeners []TaskListener
}

func NewTaskQueue(workers int) *TaskQueue {
	q := new(TaskQueue)
	q.jobs = make(chan Task)

	var ctx context.Context
	ctx, q.cancel = context.WithCancel(context.Background())

	for i := 0; i < workers; i++ {
		// Start worker here.
		go func(id int) {
			wCtx := context.WithValue(ctx, "WORKER_ID", id)

			// process jobs all my life
			for job := range q.jobs {
				q.notifyListeners("run", id, job)
				resp, err := job.Do(wCtx)
				q.notifyListeners("complete", id, job, resp, err)
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
	log.Println("[INFO] TaskQueue - close jobs channel.")
	close(q.jobs)

	log.Println("[INFO] TaskQueue - cancel workers context.")
	q.cancel()

	log.Println("[INFO] TaskQueue - wait for workers to finish.")
	if !WaitTimeout(&q.wg, 5*time.Second) {
		log.Println("[INFO] TaskQueue - WaitGroup timeout.")
	}
}

func (q *TaskQueue) Enqueue(task Task, timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return errors.New("TIMEOUT")
	case q.jobs <- task:
		q.notifyListeners("enqueue", -1, task)
		return nil
	}
}

func (q *TaskQueue) Size() int {
	return len(q.jobs)
}

func (q *TaskQueue) AddListener(l TaskListener) {
	q.listeners = append(q.listeners, l)
}

func (q *TaskQueue) notifyListeners(event string, workerID int, task Task, args ...interface{}) {
	for i := range q.listeners {
		switch event {
		case "enqueue":
			q.listeners[i].OnTaskEnqueue(task)
		case "run":
			q.listeners[i].OnTaskRun(workerID, task)
		case "complete":
			err, _ := args[1].(error)
			q.listeners[i].OnTaskComplete(workerID, task, args[0], err)
		}
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
