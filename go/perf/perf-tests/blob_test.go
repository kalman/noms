// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package perftest

import (
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/go/types"
)

const (
	KB = uint64(1000)
	MB = 1000 * KB
)

// TODO: This should be CSV, not blob.
type blobSuite struct {
	PerfSuite
	// TODO: Is sf-crime a big enough dataset? Maybe we should be using the chi building violations.
	sfCrimeBlob types.Blob
	sfCrimeFile string
}

func (suite *blobSuite) Test01ReadSFCrimeRaw() {
	assert := suite.NewAssert()

	// The SF crime data is split into a bunch of files 2016-07-28.csv.a, 2016-07-28.csv.b, etc.
	suite.Pause(func() {
		glob, err := filepath.Glob(path.Join(suite.AtticLabs, "testdata", "sf-crime", "2016-07-28.*"))
		assert.NoError(err)

		parts := make([]io.Reader, len(glob))
		for i, m := range glob {
			r, err := os.Open(m)
			assert.NoError(err)
			defer r.Close()
			parts[i] = r
		}

		whole := io.MultiReader(parts...)
		suite.sfCrimeBlob = types.NewBlob(whole)
	})

	// TODO: Use HTTP store.
	io.Copy(NopWriter{}, suite.sfCrimeBlob.Reader())
}

func (suite *blobSuite) Test02WriteSFCrime() {
	assert := suite.NewAssert()
	assert.NotNil(suite.sfCrimeBlob)

	var f *os.File

	suite.Pause(func() {
		f = suite.TempFile()
		suite.sfCrimeFile = f.Name()
	})
	defer suite.Pause(func() {
		assert.NoError(f.Close())
	})

	// TODO: Use HTTP store.
	io.Copy(f, suite.sfCrimeBlob.Reader())
}

func (suite *blobSuite) Test03ImportSFCrimeCSV() {
	assert := suite.NewAssert()
	assert.NotEqual("", suite.sfCrimeFile)

	var csvImportCmd *exec.Cmd

	suite.Pause(func() {
		// Trick the temp file logic into creating a unique path for the csv-import binary.
		f := suite.TempFile()
		f.Close()
		out := f.Name()
		os.Remove(out)

		assert.NoError(exec.Command("go", "build", "-o", out, "github.com/attic-labs/noms/samples/go/csv/csv-import").Run())
		// TODO: Use HTTP store.
		csvImportCmd = exec.Command(out, suite.sfCrimeFile, "mem::csv")
	})

	assert.NoError(csvImportCmd.Run())
}

func TestBlobPerf(t *testing.T) {
	Run(t, &blobSuite{})
}
