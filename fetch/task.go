package fetch

import (
	"context"
	"crypto"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"

	"github.com/cavaliergopher/grab/v3"
	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/common"
)

type Task interface {
	Do(context.Context) (interface{}, error)
}

type FetchTask struct {
	Sha1    string
	Dest    string
	factory *TaskFactory
	url     string
}

func (t *FetchTask) String() string {
	return fmt.Sprintf("FetchTask %s", t.Sha1)
}

func (t *FetchTask) Do(ctx context.Context) (interface{}, error) {
	var err error
	var resp interface{}
	var url string

	// Find the first origin with the file and try to download.
	// If download wasn't successful then go on to the next.
	for i := 0; i < len(t.factory.origins); i++ {
		url, err = t.factory.origins[i].RegisterFile(ctx, t.Sha1)
		if err != nil {
			log.Printf("[WARNING] Worker %v: Origin [%d] register error %s: %s\n",
				ctx.Value("WORKER_ID"), i, t, err.Error())
			continue
		}

		resp, err = t.doGrab(ctx, t.Dest, url)
		if err != nil {
			log.Printf("[WARNING] Worker %v: Download error %s: %s\n",
				ctx.Value("WORKER_ID"), t, err.Error())
			continue
		} else {
			err = nil
			break
		}
	}

	t.url = url
	return resp, err
}

func (t *FetchTask) OriginUrl() string {
	return t.url
}

func (t *FetchTask) doGrab(ctx context.Context, dst string, url string) (*grab.Response, error) {
	req, err := grab.NewRequest(dst, url)
	if err != nil {
		return nil, errors.Wrap(err, "grab.NewRequest")
	}

	// we want our own times
	req.IgnoreRemoteTime = true

	// require HEAD request to origin which is not allowed at the moment
	req.NoResume = true

	// enable checksum validation
	sha1sum, err := hex.DecodeString(t.Sha1)
	req.SetChecksum(crypto.SHA1.New(), sha1sum, true)

	// propagate context for cancellation
	req = req.WithContext(ctx)

	// actual http call
	resp := t.factory.grabber.Do(req)

	// blocking wait for complete or error
	if err := resp.Err(); err != nil {
		if resp.HTTPResponse.StatusCode == http.StatusNotFound {
			return nil, errors.Errorf("%s 404 Not Found", url)
		}
		return nil, errors.Wrapf(err, "%s resp.Err() %s", url, LogHttpResponse(resp.HTTPResponse))
	}

	return resp, nil
}

type TaskFactory struct {
	origins []*FilerBackend
	grabber *grab.Client
}

func NewTaskFactory() *TaskFactory {
	f := new(TaskFactory)

	ua := fmt.Sprintf("mdb-fs [%s]", common.Config.SuitcaseID)

	f.origins = make([]*FilerBackend, 0)
	for _, origin := range common.Config.Origins {
		f.origins = append(f.origins, NewFilerBackend(origin, ua))
	}

	f.grabber = grab.NewClient()
	f.grabber.UserAgent = ua

	return f
}

func (f *TaskFactory) Make(sha1 string, dest string) *FetchTask {
	return &FetchTask{
		factory: f,
		Sha1:    sha1,
		Dest:    dest,
	}
}
