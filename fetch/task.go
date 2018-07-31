package fetch

import (
	"context"
	"crypto"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"

	"github.com/cavaliercoder/grab"
	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/config"
)

type Task interface {
	Do(context.Context) error
}

type FetchTask struct {
	Sha1    string
	Dest    string
	factory *TaskFactory
}

func (t FetchTask) String() string {
	return fmt.Sprintf("FetchTask %s", t.Sha1)
}

func (t FetchTask) Do(ctx context.Context) error {
	// Find the first origin with the file and try to download.
	// If download wasn't successful then go on to the next.
	var url string
	var err error
	for i := 0; i < len(t.factory.origins); i++ {
		url, err = t.factory.origins[i].RegisterFile(ctx, t.Sha1)
		if err != nil {
			log.Printf("%s [worker %v]: Origin [%d] register error: %s\n",
				t, ctx.Value("WORKER_ID"), i, err.Error())
			continue
		}

		err = t.doGrab(ctx, t.Dest, url)
		if err != nil {
			log.Printf("Download error [worker %v]: %s\n",
				ctx.Value("WORKER_ID"), err.Error())
			continue
		} else {
			err = nil
			break
		}
	}

	return err
}

func (t FetchTask) doGrab(ctx context.Context, dst string, url string) error {
	req, err := grab.NewRequest(dst, url)
	if err != nil {
		return errors.Wrap(err, "grab.NewRequest")
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
			return errors.Errorf("%s 404 Not Found", url)
		}
		return errors.Wrapf(err, "%s resp.Err() %s", url, LogHttpResponse(resp.HTTPResponse))
	}

	log.Printf("[worker %v] Downloaded %s %s %.f[KBps] \n",
		ctx.Value("WORKER_ID"), url, resp.Duration(), resp.BytesPerSecond()/1024)

	return nil
}

type TaskFactory struct {
	cfg     *config.Config
	origins []*FilerBackend
	grabber *grab.Client
}

func NewTaskFactory(cfg *config.Config) *TaskFactory {
	f := new(TaskFactory)
	f.cfg = cfg

	ua := fmt.Sprintf("mdb-fs [%s]", cfg.SuitcaseID)

	f.origins = make([]*FilerBackend, len(cfg.Origins))
	for i := range cfg.Origins {
		f.origins[i] = NewFilerBackend(cfg.Origins[i], ua)
	}

	f.grabber = grab.NewClient()
	f.grabber.UserAgent = ua

	return f
}

func (f *TaskFactory) Make(sha1 string, dest string) FetchTask {
	return FetchTask{
		factory: f,
		Sha1:    sha1,
		Dest:    dest,
	}
}
