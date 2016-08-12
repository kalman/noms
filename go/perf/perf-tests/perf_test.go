// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// TODO: Directory should agree with the package name.
// TODO: Rename this file perf_suite.go.
package perftest

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
)

var (
	perfFlag        = flag.String("perf", "", "The dataset to write perf tests to. If this isn't specified, perf tests are skipped")
	perfVerboseFlag = flag.Bool("perf-verbose", false, "Make perf tests verbose")
	testNamePattern = regexp.MustCompile("^Test([A-Z0-9].*)$")
)

type PerfSuiteT interface {
	Suite() *PerfSuite
}

type PerfSuite struct {
	T         *testing.T
	AtticLabs string
	tempFiles []*os.File
	pauses    []time.Duration
}

type timeInfo struct {
	elapsed, paused, total time.Duration
}

func Run(t *testing.T, suiteT PerfSuiteT) {
	if *perfFlag == "" {
		return
	}

	assert := assert.New(t)

	ds, err := spec.GetDataset(*perfFlag)
	assert.NoError(err)
	db := ds.Database()

	suite := suiteT.Suite()
	suite.T = t
	suite.AtticLabs = path.Join(os.Getenv("GOPATH"), "src", "github.com", "attic-labs")

	tests := map[string]timeInfo{}

	defer func() {
		for _, f := range suite.tempFiles {
			os.Remove(f.Name())
		}

		timesSlice := []types.Value{}
		for name, info := range tests {
			timesSlice = append(timesSlice, types.String(name), types.NewStruct("", types.StructData{
				"elapsed": types.Number(info.elapsed.Nanoseconds()),
				"paused":  types.Number(info.paused.Nanoseconds()),
				"total":   types.Number(info.total.Nanoseconds()),
			}))
		}

		record := types.NewStruct("", map[string]types.Value{
			"environment":     suite.getEnvironment(),
			"nomsVersion":     types.String(suite.getGitHead(path.Join(suite.AtticLabs, "noms"))),
			"testdataVersion": types.String(suite.getGitHead(path.Join(suite.AtticLabs, "testdata"))),
			"tests":           types.NewMap(timesSlice...),
		})

		var err error
		ds, err = ds.CommitValue(record)
		assert.NoError(err)
		assert.NoError(db.Close())
	}()

	for t, i := reflect.TypeOf(suiteT), 0; i < t.NumMethod(); i++ {
		m := t.Method(i)

		match := testNamePattern.FindStringSubmatch(m.Name)
		if match == nil {
			continue
		}

		name := match[1]
		if *perfVerboseFlag {
			fmt.Printf("%s: running\n", name)
		}

		start := time.Now()
		suite.pauses = nil

		callSafe(name, m.Func, suiteT)

		paused := time.Duration(0)
		for _, p := range suite.pauses {
			paused += p
		}

		elapsed := time.Since(start) - paused

		if *perfVerboseFlag {
			fmt.Printf("%s: took %s (paused for %s)\n", name, elapsed, paused)
		}

		tests[name] = timeInfo{elapsed, paused, elapsed + paused}
	}
}

func (suite *PerfSuite) Suite() *PerfSuite {
	return suite
}

func (suite *PerfSuite) NewAssert() *assert.Assertions {
	return assert.New(suite.T)
}

func (suite *PerfSuite) TempFile() *os.File {
	f, err := ioutil.TempFile("", "perf-tests")
	assert.NoError(suite.T, err)
	suite.tempFiles = append(suite.tempFiles, f)
	return f
}

func (suite *PerfSuite) Pause(while func()) {
	start := time.Now()
	while()
	suite.pauses = append(suite.pauses, time.Since(start))
}

func callSafe(name string, fun reflect.Value, args ...interface{}) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "%s: error %#v\n", name, r)
		}
	}()
	rargs := make([]reflect.Value, len(args))
	for i, arg := range args {
		rargs[i] = reflect.ValueOf(arg)
	}
	fun.Call(rargs)
}

func (suite *PerfSuite) getEnvironment() types.Struct {
	assert := suite.NewAssert()

	// CPU
	cpuInfo, err := cpu.Info()
	assert.NoError(err)

	cpus := types.NewList()
	for _, c := range cpuInfo {
		c.Flags = nil // don't care about flags, and there's a lot of them
		cpus = cpus.Append(structToNoms(c))
	}

	// Memory
	vmStat, err := mem.VirtualMemory()
	assert.NoError(err)
	mem := structToNoms(*vmStat)

	// Host info
	hostInfo, err := host.Info()
	assert.NoError(err)
	host := structToNoms(*hostInfo)

	return types.NewStruct("", types.StructData{
		"cpus": cpus,
		"mem":  mem,
		"host": host,
	})
}

func (suite *PerfSuite) getGitHead(dir string) string {
	stdout := &bytes.Buffer{}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Stdout = stdout
	cmd.Dir = dir
	assert.NoError(suite.T, cmd.Run())
	return strings.TrimSpace(stdout.String())
}

func structToNoms(strct interface{}) types.Struct {
	t := reflect.TypeOf(strct)
	v := reflect.ValueOf(strct)
	d := types.StructData{}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		var nomsValue types.Value
		switch t := fieldValue.Interface().(type) {
		case int:
			nomsValue = types.Number(t)
		case int8:
			nomsValue = types.Number(t)
		case int16:
			nomsValue = types.Number(t)
		case int32:
			nomsValue = types.Number(t)
		case int64:
			nomsValue = types.Number(t)
		case uint:
			nomsValue = types.Number(t)
		case uint8:
			nomsValue = types.Number(t)
		case uint16:
			nomsValue = types.Number(t)
		case uint32:
			nomsValue = types.Number(t)
		case uint64:
			nomsValue = types.Number(t)
		case float32:
			nomsValue = types.Number(t)
		case float64:
			nomsValue = types.Number(t)
		case string:
			if t != "" {
				nomsValue = types.String(t)
			}
		}

		if nomsValue != nil {
			d[field.Name] = nomsValue
		}
	}

	return types.NewStruct("", d)
}
