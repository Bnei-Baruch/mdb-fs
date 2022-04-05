package importer

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Bnei-Baruch/mdb-fs/core"
)

func TestFolderReshaper_ReshapeRename(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	core.NewTestConfig()
	defer core.CleanupTestConfig()

	testData, err := prepareTestData()
	defer testData.Cleanup()
	r.Nil(err)

	folderReshaper := &FolderReshaper{
		syncer: core.NewMockSyncer(testData),
	}

	err = folderReshaper.Reshape(testData.SrcDir, ModeRename)
	r.Nil(err)

	err = folderReshaper.syncer.GetFS().ScanReap()
	r.Nil(err)
	idx, err := folderReshaper.syncer.GetFS().ReadIndex()
	r.Nil(err)
	for k, v := range testData.Files {
		_, ok := idx[k]
		a.Equal(v.MdbID > 0 && !v.LocalCopy, ok, "in index: %s", k)

		_, err := os.Stat(v.Path)
		a.Equal(v.LocalCopy || v.MdbID > 0, os.IsNotExist(err), "os.IsNotExist %s", v.Path)
	}
}

func TestFolderReshaper_ReshapeLink(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	core.NewTestConfig()
	defer core.CleanupTestConfig()

	testData, err := prepareTestData()
	defer testData.Cleanup()
	r.Nil(err)

	folderReshaper := &FolderReshaper{
		syncer: core.NewMockSyncer(testData),
	}

	err = folderReshaper.Reshape(testData.SrcDir, ModeLink)
	r.Nil(err)

	err = folderReshaper.syncer.GetFS().ScanReap()
	r.Nil(err)
	idx, err := folderReshaper.syncer.GetFS().ReadIndex()
	r.Nil(err)
	for k, v := range testData.Files {
		_, ok := idx[k]
		a.Equal(v.MdbID > 0 && !v.LocalCopy, ok, "in index: %s", k)

		_, err := os.Stat(v.Path)
		a.Nil(err, "os.Stat %s", v.Path)
	}
}

func prepareTestData() (*core.TestData, error) {
	var err error
	data := &core.TestData{
		Files: make(map[string]*core.TestFile),
	}

	// source directory
	data.SrcDir, err = ioutil.TempDir("", "reshaper_source")
	if err != nil {
		return nil, errors.Wrap(err, "ioutil.TempDir")
	}
	fmt.Printf("source directory: %s\n", data.SrcDir)

	// files
	for i := 0; i < 4; i++ {
		f, err := core.MakeTestFile(data.SrcDir, fmt.Sprintf("test_file_%d", i), 50)
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

		data.Files[f.Sha1] = f
	}

	// write index
	idx := make(map[string]*core.FileRecord)
	for k, v := range data.Files {
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
