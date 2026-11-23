// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb/v2/prealloc"
)

func preallocStore(t *testing.T, chunkSize int64) (*BoltStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raft.db")
	store, err := New(Options{
		Path:                 path,
		PreAllocateChunkSize: chunkSize,
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	return store, path
}

func TestPreAllocate_RejectsChunkBelowMinimum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	if _, err := New(Options{
		Path:                 path,
		PreAllocateChunkSize: prealloc.MinChunkSize() - 1,
	}); err != prealloc.ErrInvalidChunkSize {
		t.Fatalf("expected ErrInvalidChunkSize, got %v", err)
	}
}

func TestPreAllocate_ZeroKeepsDefaultBehavior(t *testing.T) {
	store, path := preallocStore(t, 0)
	defer store.Close()
	defer os.Remove(path)

	if err := store.StoreLog(testRaftLog(1, "log1")); err != nil {
		t.Fatalf("err: %s", err)
	}
	got := new(raft.Log)
	if err := store.GetLog(1, got); err != nil {
		t.Fatalf("err: %s", err)
	}
	if got.Index != 1 {
		t.Fatalf("bad index: %d", got.Index)
	}
}

func TestPreAllocate_StoresAndReadsLogs(t *testing.T) {
	const chunkSize = 4 * 1024 * 1024
	store, path := preallocStore(t, chunkSize)
	defer os.Remove(path)

	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first != 1 || last != 3 {
		t.Fatalf("indexes = [%d,%d], want [1,3]", first, last)
	}

	for _, want := range logs {
		got := new(raft.Log)
		if err := store.GetLog(want.Index, got); err != nil {
			t.Fatalf("get log %d: %s", want.Index, err)
		}
		if string(got.Data) != string(want.Data) {
			t.Fatalf("log %d data = %q, want %q", want.Index, got.Data, want.Data)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestPreAllocate_ReopenPreservesLogs(t *testing.T) {
	const chunkSize = 2 * 1024 * 1024
	store, path := preallocStore(t, chunkSize)

	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	reopened, err := New(Options{
		Path:                 path,
		PreAllocateChunkSize: chunkSize,
	})
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	defer reopened.Close()
	defer os.Remove(path)

	last, err := reopened.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if last != 2 {
		t.Fatalf("last index after reopen = %d, want 2", last)
	}

	got := new(raft.Log)
	if err := reopened.GetLog(1, got); err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(got.Data) != "log1" {
		t.Fatalf("log data after reopen = %q, want log1", got.Data)
	}
}

func TestPreAllocate_FileGrowsInBoundedChunks(t *testing.T) {
	const chunkSize = 2 * 1024 * 1024
	store, path := preallocStore(t, chunkSize)
	defer store.Close()
	defer os.Remove(path)

	// Write enough logs to span multiple chunks. The high-water mark must always
	// be an exact multiple of the chunk size instead of doubling.
	for i := uint64(1); i <= 200; i++ {
		entry := testRaftLog(i, string(make([]byte, 16*1024)))
		if err := store.StoreLog(entry); err != nil {
			t.Fatalf("store log %d: %s", i, err)
		}

		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %s", err)
		}
		if info.Size()%chunkSize != 0 {
			t.Fatalf("file size %d is not a multiple of chunk %d", info.Size(), chunkSize)
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %s", err)
	}
	// 200 * 16 KiB = ~3.1 MiB of payload, so the file must not balloon through
	// bbolt's doubling strategy beyond a small, bounded number of chunks.
	maxSize := int64(8) * chunkSize
	if info.Size() > maxSize {
		t.Fatalf("file size %d exceeds bounded limit %d", info.Size(), maxSize)
	}
}
