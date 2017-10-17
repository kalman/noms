// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}

	verbose.RegisterVerboseFlags(flag.CommandLine)

	flag.Parse(true)

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "Missing required dataset argument")
		return
	}

	cfg := config.NewResolver()
	db, ds, err := cfg.GetDataset(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
		return
	}
	defer db.Close()

	l := types.NewList(db).Edit()
	for i := uint64(0); i < 10000000; i++ {
		l.Append(types.String("yellow"))
	}

	_, err = db.CommitValue(ds, l.Value())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error committing: %s\n", err)
		return
	}
}
