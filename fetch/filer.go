package fetch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type FileBackendRequest struct {
	SHA1 string `json:"sha1"`
	Name string `json:"name"`
	//ClientIP string `json:"clientip,omitempty"`
}

type FileBackendResponse struct {
	Url string `json:"url"`
}

type FilerBackend struct {
	Url       string
	UserAgent string
	client    *http.Client
}

func NewFilerBackend(url string, ua string) *FilerBackend {
	fb := new(FilerBackend)
	fb.Url = url
	fb.UserAgent = ua
	fb.client = &http.Client{
		Timeout: 5 * time.Second,
	}
	return fb
}

func (fb *FilerBackend) RegisterFile(ctx context.Context, sha1 string) (string, error) {
	// prepare request
	data := FileBackendRequest{
		SHA1: sha1,
		Name: "mdb-fs",
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(data); err != nil {
		return "", errors.Wrap(err, "json.Encode")
	}

	url := fmt.Sprintf("%s/api/v1/get", fb.Url)
	req, err := http.NewRequest(http.MethodPost, url, b)
	if err != nil {
		return "", errors.Wrap(err, "http.NewRequest")
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("User-Agent", fb.UserAgent)

	req = req.WithContext(ctx)

	// actual http call
	//log.Printf("Register file access %s %s\n", fb.Url, sha1)
	resp, err := fb.client.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "client.Do")
	}

	// Physical file exists
	if resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()

		var body FileBackendResponse
		err := json.NewDecoder(resp.Body).Decode(&body)
		if err != nil {
			return "", errors.Wrap(err, "json.Decode")
		}

		return body.Url, nil
	}

	// physical file doesn't exists
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound{
		return "", errors.New("NOT_FOUND")
	}

	// Unexpected response
	return "", errors.New(LogHttpResponse(resp))
}
