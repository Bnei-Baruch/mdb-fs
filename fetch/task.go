package fetch

import (
	"context"
	"crypto"
	"fmt"
	"log"
	"path/filepath"

	"github.com/cavaliercoder/grab"
	"github.com/pkg/errors"

	"encoding/hex"
	"github.com/Bnei-Baruch/mdb-fs/config"
	"net/http"
)

type Task interface {
	Do(context.Context) error
}

type FetchTask struct {
	Sha1    string
	factory *TaskFactory
}

func (t FetchTask) String() string {
	return fmt.Sprintf("FetchTask %s", t.Sha1)
}

func (t FetchTask) Do(ctx context.Context) error {
	//time.Sleep(100 * time.Second)

	// TODO: verify file not exist before download (maybe in syncer)
	dst := filepath.Join(t.factory.cfg.RootDir, t.Sha1[0:1], t.Sha1[1:2], t.Sha1[2:3], t.Sha1)

	// Find the first origin with the file and try to download.
	// If download wasn't successful then go on to the next.
	var url string
	var err error
	for i := 0; i < len(t.factory.origins); i++ {
		url, err = t.factory.origins[i].RegisterFile(ctx, t.Sha1)
		if err != nil {
			log.Printf("%s: Origin [%d] register error: %s\n", t, i, err.Error())
			continue
		}

		err = t.doGrab(ctx, dst, url)
		if err != nil {
			log.Printf("Download error: %s\n", err.Error())
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
	req.IgnoreRemoteTime = true

	// Require HEAD request to origin which is not allowed at the moment.
	req.NoResume = true

	sha1sum, err := hex.DecodeString(t.Sha1)
	req.SetChecksum(crypto.SHA1.New(), sha1sum, true)
	req = req.WithContext(ctx)

	resp := t.factory.grabber.Do(req)

	// blocking
	if err := resp.Err(); err != nil {
		if resp.HTTPResponse.StatusCode == http.StatusNotFound {
			return errors.Errorf("%s 404 Not Found", url)
		}
		return errors.Wrapf(err, "%s resp.Err() %s", url, LogHttpResponse(resp.HTTPResponse))
	}

	log.Printf("Downloaded %s %s %.f[KBps] \n", url, resp.Duration(), resp.BytesPerSecond()/1024)

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

	f.origins = make([]*FilerBackend, len(cfg.Origins))
	for i := range cfg.Origins {
		f.origins[i] = NewFilerBackend(cfg.Origins[i])
	}

	f.grabber = grab.NewClient()
	f.grabber.UserAgent = "mdb-fs"

	return f
}

func (f *TaskFactory) Make(sha1 string) FetchTask {
	return FetchTask{
		factory: f,
		Sha1:    sha1,
	}
}
