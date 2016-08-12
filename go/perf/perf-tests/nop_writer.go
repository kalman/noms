// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package perftest

// NopWriter just consumes written bytes. Intended to be used with io.Copy for reading an entire io.Reader.
type NopWriter struct{}

func (r NopWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
