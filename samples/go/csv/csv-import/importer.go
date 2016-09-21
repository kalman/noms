// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	nomcsv "github.com/attic-labs/noms/go/csv"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/progressreader"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/samples/go/csv"
	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

const (
	destList = iota
	destMap  = iota
)

type limitReader struct {
	r   io.Reader
	lim uint64
}

func (r *limitReader) Read(buf []byte) (n int, err error) {
	if r.lim == 0 {
		return 0, io.EOF
	}
	if r.lim < uint64(len(buf)) {
		buf = buf[:int(r.lim)]
	}
	n, err = r.r.Read(buf)
	r.lim -= uint64(n)
	d.PanicIfTrue(r.lim < 0)
	return
}

func main() {
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter := flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	header := flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	name := flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
	columnTypes := flag.String("column-types", "", "a comma-separated list of types representing the desired type of each column. if absent all types default to be String")
	pathDescription := "noms path to blob to import"
	path := flag.String("path", "", pathDescription)
	flag.StringVar(path, "p", "", pathDescription)
	noProgress := flag.Bool("no-progress", false, "prevents progress from being output if true")
	destType := flag.String("dest-type", "list", "the destination type to import to. can be 'list' or 'map:<pk>', where <pk> is the index position (0-based) of the column that is a the unique identifier for the column")
	skipRecords := flag.Uint("skip-records", 0, "number of records to skip at beginning of file")
	performCommit := flag.Bool("commit", true, "commit the data to head of the dataset (otherwise only write the data to the dataset)")
	spec.RegisterCommitMetaFlags(flag.CommandLine)
	spec.RegisterDatabaseFlags(flag.CommandLine)
	profile.RegisterProfileFlags(flag.CommandLine)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: csv-import [options] <csvfile> <dataset>\n\n")
		flag.PrintDefaults()
	}

	flag.Parse(true)

	var err error
	switch {
	case flag.NArg() == 0:
		err = errors.New("Maybe you put options after the dataset?")
	case flag.NArg() == 1 && *path == "":
		err = errors.New("If <csvfile> isn't specified, you must specify a noms path with -p")
	case flag.NArg() == 2 && *path != "":
		err = errors.New("Cannot specify both <csvfile> and a noms path with -p")
	case flag.NArg() > 2:
		err = errors.New("Too many arguments")
	}
	d.CheckError(err)

	delim, err := csv.StringToRune(*delimiter)
	d.CheckErrorNoUsage(err)

	defer profile.MaybeStartProfile().Stop()

	// Analyse CSV file structure.
	// TODO: Show progress.
	r1, closer1, _, _, _ := open(*path)
	defer closer1.Close()

	rowFinder := nomcsv.NewRowFinder(r1)
	rowFinder.Comma = delim
	rowOffsets, err := rowFinder.FindAll()
	d.PanicIfError(err)

	// Read header, and possibly the rest of the file if we're not reading to a
	// list (otherwise it'll be done in parallel later).
	r2, closer2, size, filePath, dataSetArgN := open(*path)
	defer closer2.Close()

	if !*noProgress {
		r2 = progressreader.New(r2, getStatusPrinter(size))
	}

	var dest int
	var strPks []string
	if *destType == "list" {
		dest = destList
	} else if strings.HasPrefix(*destType, "map:") {
		dest = destMap
		strPks = strings.Split(strings.TrimPrefix(*destType, "map:"), ",")
		if len(strPks) == 0 {
			fmt.Println("Invalid dest-type map: ", *destType)
			return
		}
	} else {
		fmt.Println("Invalid dest-type: ", *destType)
		return
	}

	cr := csv.NewCSVReader(r2, delim)
	err = csv.SkipRecords(cr, *skipRecords)

	var rowRanges []uint64
	if *skipRecords == 0 {
		rowRanges = append(rowRanges, 0)
		rowRanges = append(rowRanges, rowOffsets...)
	} else if int(*skipRecords)-1 < len(rowOffsets) {
		rowRanges = rowOffsets[*skipRecords-1:]
	} else {
		err = io.EOF
	}

	if err == io.EOF {
		err = fmt.Errorf("skip-records skipped past EOF")
	}
	d.CheckErrorNoUsage(err)

	var headers []string
	if *header == "" {
		headers, err = cr.Read()
		d.PanicIfError(err)
		rowRanges = rowRanges[1:]
	} else {
		headers = strings.Split(*header, ",")
	}

	uniqueHeaders := make(map[string]bool)
	for _, header := range headers {
		uniqueHeaders[header] = true
	}
	if len(uniqueHeaders) != len(headers) {
		d.CheckErrorNoUsage(fmt.Errorf("Invalid headers specified, headers must be unique"))
	}

	kinds := []types.NomsKind{}
	if *columnTypes != "" {
		kinds = csv.StringsToKinds(strings.Split(*columnTypes, ","))
		if len(kinds) != len(uniqueHeaders) {
			d.CheckErrorNoUsage(fmt.Errorf("Invalid column-types specified, column types do not correspond to number of headers"))
		}
	}

	ds, err := spec.GetDataset(flag.Arg(dataSetArgN))
	d.CheckError(err)
	defer ds.Database().Close()

	var value types.Value
	if dest == destList && len(rowRanges) > 1 {
		// Parallel, baby.
		// TODO: Base on a -p flag or number of CPU cores, not just 2.
		fstStart, sndStart := rowRanges[0], rowRanges[len(rowRanges)/2]
		var fst, snd types.List
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			// TODO: Progress?
			r, closer, _, _, _ := open(*path)
			defer closer.Close()
			_, err := r.Seek(int64(fstStart), 0)
			d.PanicIfError(err)
			cr = csv.NewCSVReader(&limitReader{r, sndStart - fstStart}, delim)
			fst, _ = csv.ReadToList(cr, *name, headers, kinds, ds.Database())
			wg.Done()
		}()
		go func() {
			// TODO: Progress?
			r, closer, _, _, _ := open(*path)
			defer closer.Close()
			_, err := r.Seek(int64(sndStart), 0)
			d.PanicIfError(err)
			cr = csv.NewCSVReader(r, delim)
			snd, _ = csv.ReadToList(cr, *name, headers, kinds, ds.Database())
			wg.Done()
		}()
		wg.Wait()
		value = fst.Concat(snd)
	} else if dest == destList {
		value, _ = csv.ReadToList(cr, *name, headers, kinds, ds.Database())
	} else {
		value = csv.ReadToMap(cr, *name, headers, strPks, kinds, ds.Database())
	}

	if *performCommit {
		meta, err := spec.CreateCommitMetaStruct(ds.Database(), "", "", additionalMetaInfo(filePath, *path), nil)
		d.CheckErrorNoUsage(err)
		_, err = ds.Commit(value, dataset.CommitOptions{Meta: meta})
		if !*noProgress {
			status.Clear()
		}
		d.PanicIfError(err)
	} else {
		ref := ds.Database().WriteValue(value)
		if !*noProgress {
			status.Clear()
		}
		fmt.Fprintf(os.Stdout, "#%s\n", ref.TargetHash().String())
	}
}

func additionalMetaInfo(filePath, nomsPath string) map[string]string {
	fileOrNomsPath := "inputPath"
	path := nomsPath
	if path == "" {
		path = filePath
		fileOrNomsPath = "inputFile"
	}
	return map[string]string{fileOrNomsPath: path}
}

func getStatusPrinter(expected uint64) progressreader.Callback {
	startTime := time.Now()
	return func(seen uint64) {
		percent := float64(seen) / float64(expected) * 100
		elapsed := time.Since(startTime)
		rate := float64(seen) / elapsed.Seconds()

		status.Printf("%.2f%% of %s (%s/s)...",
			percent,
			humanize.Bytes(expected),
			humanize.Bytes(uint64(rate)))
	}
}

func open(path string) (io.ReadSeeker, io.Closer, uint64, string, int) {
	var r io.ReadSeeker
	var closer io.Closer
	var size uint64
	var filePath string
	var dataSetArgN int

	if path != "" {
		db, val, err := spec.GetPath(path)
		d.CheckError(err)
		if val == nil {
			d.CheckError(fmt.Errorf("Path %s not found\n", path))
		}
		blob, ok := val.(types.Blob)
		if !ok {
			d.CheckError(fmt.Errorf("Path %s not a Blob: %s\n", path, types.EncodedValue(val.Type())))
		}
		r = blob.Reader()
		closer = db
		size = blob.Len()
		dataSetArgN = 0
	} else {
		filePath = flag.Arg(0)
		res, err := os.Open(filePath)
		d.CheckError(err)
		fi, err := res.Stat()
		d.CheckError(err)
		r = res
		closer = res
		size = uint64(fi.Size())
		dataSetArgN = 1
	}

	return r, closer, size, filePath, dataSetArgN
}
