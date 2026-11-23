// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

// Package prealloc grows data files in fixed-size chunks using hole-punching
// filesystem preallocation (fallocate) instead of the doubling/remap strategy
// performed by the underlying key/value engine.
//
// Background: bbolt doubles its memory map up to 1 GiB (then steps by 1 GiB)
// and, every time it grows, issues a truncate plus a full-file fsync. On a
// large log file backed by an ordinary cloud disk that grow can block the
// write path for hundreds of milliseconds, long enough to trip a Raft election
// timeout and force a leader election. The Manager holds its own file
// descriptor and reserves space slightly ahead of the engine's write position,
// so the engine's own truncate becomes a metadata no-op: reservations are
// bounded, on-demand, and never rewrite or discard already acknowledged data.
package prealloc

import (
	"errors"
	"os"
	"sync"
)

const (
	// DefaultChunkSize is the amount of space reserved per growth step. 64 MiB
	// keeps a single reservation well below typical election timeouts while
	// making the number of reservation syscalls negligible even for a log that
	// grows by hundreds of GiB per day.
	DefaultChunkSize = 64 * 1024 * 1024

	// minChunkSize guards against pathological configurations that would issue
	// a reservation syscall on virtually every write.
	minChunkSize = 1 << 20 // 1 MiB

	// headroomChunks keeps reservations one chunk ahead of the current write
	// position, so a burst of writes never has to wait for an in-line grow.
	headroomChunks = 1
)

var (
	// ErrInvalidChunkSize is returned when a configured chunk size is below the
	// supported minimum.
	ErrInvalidChunkSize = errors.New("prealloc: chunk size must be at least 1 MiB")

	// ErrClosed is returned when a reserved size is requested after Close.
	ErrClosed = errors.New("prealloc: manager closed")
)

// MinChunkSize returns the smallest supported reservation chunk.
func MinChunkSize() int64 { return minChunkSize }

// Manager reserves backing storage for a data file through a dedicated file
// descriptor. It is safe for concurrent use: bbolt keeps its own descriptor
// while the Manager only ever extends the same inode.
//
// The Manager never shrinks the file and never touches bytes below the current
// high-water mark, so acknowledged entries cannot be lost.
type Manager struct {
	// file is a dedicated descriptor used only for reservations. It is
	// distinct from the descriptor the key/value engine writes through.
	file *os.File

	// chunkSize is the granularity at which space is reserved.
	chunkSize int64

	mu sync.Mutex

	// reserved is the high-water mark of bytes guaranteed to have backing
	// storage. It is always a multiple of chunkSize and never decreases.
	reserved int64

	closed bool
}

// NewManager opens the named file (creating it if needed) and returns a
// Manager sized to the file's current length. Reopening a large log resumes the
// reservation cadence without re-reserving or copying the populated prefix.
func NewManager(path string, chunkSize int64) (*Manager, error) {
	if chunkSize < minChunkSize {
		return nil, ErrInvalidChunkSize
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &Manager{
		file:      file,
		chunkSize: chunkSize,
		reserved:  ceilChunk(info.Size(), chunkSize),
	}, nil
}

// Reserve guarantees that at least size bytes have backing storage, extending
// the file by whole chunks when necessary. It is a no-op when size is already
// covered. The returned value is the new reserved high-water mark.
func (m *Manager) Reserve(size int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, ErrClosed
	}

	// Keep one extra chunk of headroom so the engine's next truncate lands
	// inside already reserved space.
	target := ceilChunk(size+headroomChunks*m.chunkSize, m.chunkSize)
	if target <= m.reserved {
		return m.reserved, nil
	}

	if err := preallocate(m.file, m.reserved, target); err != nil {
		return m.reserved, err
	}
	m.reserved = target
	return m.reserved, nil
}

// Reserved returns the current reserved high-water mark.
func (m *Manager) Reserved() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reserved
}

// Close releases the Manager's dedicated descriptor. It does not close the
// descriptor used by the key/value engine.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	return m.file.Close()
}

// ceilChunk rounds size up to the nearest non-negative multiple of chunkSize.
func ceilChunk(size, chunkSize int64) int64 {
	if size <= 0 {
		return 0
	}
	return ((size + chunkSize - 1) / chunkSize) * chunkSize
}
