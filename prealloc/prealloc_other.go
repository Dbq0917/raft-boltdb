// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

//go:build !linux

package prealloc

import "os"

// preallocate reserves the byte range [from, to) on platforms without a
// hole-punching fallocate wrapper. ftruncate only extends the file and never
// modifies the existing [0, from) prefix, so acknowledged data is preserved.
func preallocate(file *os.File, from, to int64) error {
	if to <= from {
		return nil
	}
	return file.Truncate(to)
}
