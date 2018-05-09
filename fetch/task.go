package fetch

import (
	"fmt"
	"time"
)

type FetchTask struct {
	Sha1 string
}

func (t FetchTask) Do() error {
	time.Sleep(100 * time.Second)
	return nil
}

func (t FetchTask) String() string {
	return fmt.Sprintf("FetchTask %s", t.Sha1)
}
