// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package prealloc

import (
	"os"

	"golang.org/x/sys/unix"
)

// filesystemsWithoutFallocate list filesystem magic numbers that reject
// fallocate even though they are regular, writable filesystems. tmpfs and
// ramfs back most container /tmp mounts and return EOPNOTSUPP.
var filesystemsWithoutFallocate = map[int64]struct{}{
	unix.TMPFS_MAGIC: {},
	unix.RAMFS_MAGIC: {},
}

// preallocate reserves the byte range [from, to) without zeroing it.
//
// fallocate allocates blocks and updates the file size as a metadata-only
// operation: the kernel does not copy or zero the existing prefix, so the cost
// is bounded by the new chunk rather than the whole file. Filesystems that do
// support fallocate transparently fall back to ftruncate, which preserves all
// existing bytes.
func preallocate(file *os.File, from, to int64) error {
	if to <= from {
		return nil
	}

	if supported, err := fallocateSupported(file); err != nil {
		return err
	} else if supported {
		if err := unix.Fallocate(int(file.Fd()), 0, from, to-from); err == nil {
			return nil
		}
		// A filesystem may reject fallocate at runtime even when statfs did not
		// flag it; fall through to the portable truncate path.
	}

	return file.Truncate(to)
}

// fallocateSupported reports whether the filesystem backing file supports the
// fallocate syscall. A statfs failure is treated as "supported, try it" because
// fallocate itself remains the source of truth and will fall back on error.
func fallocateSupported(file *os.File) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &stat); err != nil {
		return true, nil
	}

	magic := int64(stat.Type)
	if _, unsupported := filesystemsWithoutFallocate[magic]; unsupported {
		return false, nil
	}
	return true, nil
}
