package importer

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Bnei-Baruch/mdb-fs/common"
	"github.com/Bnei-Baruch/mdb-fs/core"
)

func TestFolderReshaper_ReshapeRename(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	NewTestConfig()
	defer CleanupConfig()

	testData, err := prepareTestData()
	defer testData.Cleanup()
	r.Nil(err)

	folderReshaper := &FolderReshaper{
		syncer: NewMockSyncer(testData),
	}

	err = folderReshaper.Reshape(testData.srcDir, ModeRename)
	r.Nil(err)

	err = folderReshaper.syncer.GetFS().ScanReap()
	r.Nil(err)
	idx, err := folderReshaper.syncer.GetFS().ReadIndex()
	r.Nil(err)
	for k, v := range testData.files {
		_, ok := idx[k]
		a.Equal(v.MdbID > 0 && !v.LocalCopy, ok, "in index: %s", k)

		_, err := os.Stat(v.Path)
		a.Equal(v.LocalCopy || v.MdbID > 0, os.IsNotExist(err), "os.IsNotExist %s", v.Path)
	}
}

func TestFolderReshaper_ReshapeLink(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	NewTestConfig()
	defer CleanupConfig()

	testData, err := prepareTestData()
	defer testData.Cleanup()
	r.Nil(err)

	folderReshaper := &FolderReshaper{
		syncer: NewMockSyncer(testData),
	}

	err = folderReshaper.Reshape(testData.srcDir, ModeLink)
	r.Nil(err)

	err = folderReshaper.syncer.GetFS().ScanReap()
	r.Nil(err)
	idx, err := folderReshaper.syncer.GetFS().ReadIndex()
	r.Nil(err)
	for k, v := range testData.files {
		_, ok := idx[k]
		a.Equal(v.MdbID > 0 && !v.LocalCopy, ok, "in index: %s", k)

		_, err := os.Stat(v.Path)
		a.Nil(err, "os.Stat %s", v.Path)
	}

}

type TestFile struct {
	core.FileRecord
	Path string
}

type TestData struct {
	srcDir string
	files  map[string]*TestFile
}

func prepareTestData() (*TestData, error) {
	var err error
	data := &TestData{
		files: make(map[string]*TestFile),
	}

	// source directory
	data.srcDir, err = ioutil.TempDir("", "reshaper_source")
	if err != nil {
		return nil, errors.Wrap(err, "ioutil.TempDir")
	}
	fmt.Printf("source directory: %s\n", data.srcDir)

	// files
	for i := 0; i < 4; i++ {
		f, err := makeTestFile(data.srcDir)
		if err != nil {
			return nil, errors.WithMessage(err, "make test file")
		}

		switch i % 4 {
		case 1:
			f.MdbID = int64(i)
		case 2:
			f.LocalCopy = true
		case 3:
			f.MdbID = int64(i)
			f.LocalCopy = true
		}

		data.files[f.Sha1] = f
	}

	// write index
	idx := make(map[string]*core.FileRecord)
	for k, v := range data.files {
		if v.LocalCopy {
			idx[k] = &v.FileRecord
		}
	}

	fs := core.NewSha1FS()
	err = fs.WriteIndex(idx)
	if err != nil {
		return nil, errors.WithMessage(err, "fs.WriteIndex")
	}

	return data, nil
}

func makeTestFile(dir string) (*TestFile, error) {
	f, err := ioutil.TempFile(dir, "")
	if err != nil {
		return nil, errors.Wrap(err, "ioutil.TempFile")
	}
	defer f.Close()

	b := make([]byte, 50)
	_, err = rand.Read(b)
	if err != nil {
		return nil, errors.Wrap(err, "rand.Read")
	}

	_, err = f.Write(b)
	if err != nil {
		return nil, errors.Wrap(err, "f.Write")
	}

	hashInBytes := sha1.Sum(b)
	hashStr := hex.EncodeToString(hashInBytes[:])

	fInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, errors.Wrap(err, "os.Stat")
	}
	return &TestFile{
		FileRecord: core.FileRecord{
			Sha1:    hashStr,
			Size:    50,
			ModTime: fInfo.ModTime(),
		},
		Path: f.Name(),
	}, nil
}

func (d *TestData) Cleanup() error {
	return os.RemoveAll(d.srcDir)
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

func CleanupConfig() error {
	return os.RemoveAll(common.Config.RootDir)
}

type MockSyncer struct {
	*core.SyncerImpl
	testData *TestData
}

func NewMockSyncer(testData *TestData) *MockSyncer {
	return &MockSyncer{
		SyncerImpl: &core.SyncerImpl{
			FS: core.NewSha1FS(),
		},
		testData: testData,
	}
}

func (s *MockSyncer) AugmentMDBToIndex(idx map[string]*core.FileRecord) error {
	for k, v := range s.testData.files {
		if r, ok := idx[k]; ok {
			r.MdbID = v.MdbID
		} else if v.MdbID > 0 {
			idx[k] = &v.FileRecord
		}
	}

	return nil
}
