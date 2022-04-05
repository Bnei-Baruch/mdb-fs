package core

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/Bnei-Baruch/mdb-fs/common"
)

type TestFile struct {
	FileRecord
	Path string
}

type TestData struct {
	SrcDir string
	Files  map[string]*TestFile
}

func (d *TestData) Cleanup() error {
	return os.RemoveAll(d.SrcDir)
}

func NewTestConfig() {
	rootDir, err := ioutil.TempDir("", "root_dir")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Root directory: %s\n", rootDir)

	common.Init()
	common.Config.RootDir = rootDir
	common.Config.MDBUrl = ""
	common.Config.Origins = []string{}
	common.Config.Fetchers = 1
	common.Config.IndexWorkers = 1
	common.Config.SyncUpdateInterval = time.Hour
	common.Config.SuitcaseID = "test"
}

func CleanupTestConfig() error {
	return os.RemoveAll(common.Config.RootDir)
}

type MockSyncer struct {
	*SyncerImpl
	testData *TestData
}

func NewMockSyncer(testData *TestData) *MockSyncer {
	return &MockSyncer{
		SyncerImpl: &SyncerImpl{
			FS: NewSha1FS(),
		},
		testData: testData,
	}
}

func (s *MockSyncer) AugmentMDBToIndex(idx map[string]*FileRecord) error {
	for k, v := range s.testData.Files {
		if r, ok := idx[k]; ok {
			r.MdbID = v.MdbID
		} else if v.MdbID > 0 {
			idx[k] = &v.FileRecord
		}
	}

	return nil
}

func MakeTestFile(dir, name string, size int) (*TestFile, error) {
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		return nil, errors.Wrap(err, "rand.Read")
	}

	hashInBytes := sha1.Sum(b)
	hashStr := hex.EncodeToString(hashInBytes[:])

	if name == "" {
		name = hashStr
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, errors.Wrap(err, "os.Open")
	}
	_, err = f.Write(b)
	if err != nil {
		return nil, errors.Wrap(err, "f.Write")
	}
	f.Close()

	fInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, errors.Wrap(err, "os.Stat")
	}
	return &TestFile{
		FileRecord: FileRecord{
			Sha1:    hashStr,
			Size:    fInfo.Size(),
			ModTime: fInfo.ModTime(),
		},
		Path: f.Name(),
	}, nil
}
