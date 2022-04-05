package core

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Bnei-Baruch/mdb-fs/common"
)

func TestSha1FS_ScanReap(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	NewTestConfig()
	defer CleanupTestConfig()

	testData, err := prepareTestData()
	defer testData.Cleanup()
	r.Nil(err)

	fs := NewSha1FS()

	idx := make(map[string]*FileRecord)
	err = fs.WriteIndex(idx)
	r.Nil(err)

	err = fs.ScanReap()
	r.Nil(err)

	idx, err = fs.ReadIndex()
	r.Nil(err)
	for k, v := range testData.Files {
		fr, ok := idx[k]
		r.Truef(ok, "file in index %s", k)
		a.Equal(v.Sha1, fr.Sha1, "sha1")
		a.Equal(v.Size, fr.Size, "size")
		a.Equal(v.ModTime.Unix(), fr.ModTime.Unix(), "ModTime")
	}
}

func TestSha1FS_ScanReapPartial(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	NewTestConfig()
	defer CleanupTestConfig()

	testData, err := prepareTestData()
	defer testData.Cleanup()
	r.Nil(err)

	fs := NewSha1FS()

	idx := make(map[string]*FileRecord)
	err = fs.WriteIndex(idx)
	r.Nil(err)

	completedTasks := NewCompletedChecksumTasks(fs.Root)
	r.Nil(completedTasks.load())
	for _, v := range testData.Files {
		r.Nil(completedTasks.add(&ChecksumTask{
			Path:     v.Path,
			Checksum: v.Sha1,
			FName:    v.Sha1,
			FSize:    v.Size,
			FModTime: v.ModTime.Unix(),
		}))
	}
	completedTasks.Close()

	err = fs.ScanReap()
	r.Nil(err)

	idx, err = fs.ReadIndex()
	r.Nil(err)
	for k, v := range testData.Files {
		fr, ok := idx[k]
		r.Truef(ok, "file in index %s", k)
		a.Equal(v.Sha1, fr.Sha1, "sha1")
		a.Equal(v.Size, fr.Size, "size")
		a.Equal(v.ModTime.Unix(), fr.ModTime.Unix(), "ModTime")
	}
}

func prepareTestData() (*TestData, error) {
	var err error
	data := &TestData{
		Files: make(map[string]*TestFile),
	}

	// source directory
	data.SrcDir, err = ioutil.TempDir("", "scan_reap")
	if err != nil {
		return nil, errors.Wrap(err, "ioutil.TempDir")
	}
	fmt.Printf("source directory: %s\n", data.SrcDir)
	common.Config.RootDir = data.SrcDir

	// files
	for i := 0; i < 4; i++ {
		f, err := MakeTestFile(data.SrcDir, "", 50)
		if err != nil {
			return nil, errors.WithMessage(err, "make test file")
		}
		data.Files[f.Sha1] = f
	}

	return data, nil
}
