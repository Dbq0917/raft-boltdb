// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package prealloc

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempPath(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "prealloc.db")
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

func TestNewManager_RejectsSmallChunk(t *testing.T) {
	if _, err := NewManager(tempPath(t), minChunkSize-1); err != ErrInvalidChunkSize {
		t.Fatalf("expected ErrInvalidChunkSize, got %v", err)
	}
}

func TestReserve_GrowsInChunksWithHeadroom(t *testing.T) {
	const chunk = 4 * 1024 * 1024
	mgr, err := NewManager(tempPath(t), chunk)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	// Requesting the first byte reserves the requested chunk plus one chunk of
	// headroom, so two chunks total.
	got, err := mgr.Reserve(1)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if want := int64(2 * chunk); got != want || mgr.Reserved() != want {
		t.Fatalf("reserved = %d, want %d", got, want)
	}

	// Requests inside the reserved region are no-ops.
	got, err = mgr.Reserve(chunk)
	if err != nil {
		t.Fatalf("reserve inside: %v", err)
	}
	if want := int64(2 * chunk); got != want {
		t.Fatalf("reserved changed to %d, want %d", got, want)
	}

	// Crossing the boundary adds exactly one more chunk.
	got, err = mgr.Reserve(2*chunk + 1)
	if err != nil {
		t.Fatalf("reserve cross: %v", err)
	}
	if want := int64(4 * chunk); got != want {
		t.Fatalf("reserved = %d, want %d", got, want)
	}

	info, err := os.Stat(mgr.file.Name())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 4*chunk {
		t.Fatalf("file size = %d, want %d", info.Size(), 4*chunk)
	}
}

func TestReserve_PreservesDataWrittenByAnotherHandle(t *testing.T) {
	const chunk = 2 * 1024 * 1024
	path := tempPath(t)

	// A separate descriptor stands in for the key/value engine writing real
	// data through its own file handle.
	writer, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()

	mgr, err := NewManager(path, chunk)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	acknowledged := []byte("acknowledged-log-entry")
	if _, err := writer.WriteAt(acknowledged, 0); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Force the manager to reserve across several chunk boundaries.
	if _, err := mgr.Reserve(3 * chunk); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	got := make([]byte, len(acknowledged))
	if _, err := writer.ReadAt(got, 0); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != string(acknowledged) {
		t.Fatalf("existing data corrupted: got %q want %q", got, acknowledged)
	}
}

func TestNewManager_ReopenKeepsHighWaterMark(t *testing.T) {
	const chunk = 2 * 1024 * 1024
	path := tempPath(t)

	first, err := NewManager(path, chunk)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := first.Reserve(chunk + 1); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := NewManager(path, chunk)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()

	// The first reservation of (chunk+1) rounded up plus one headroom chunk
	// landed at three chunks; reopening must resume from that high-water mark.
	if got, want := second.Reserved(), int64(3*chunk); got != want {
		t.Fatalf("reopen reserved = %d, want %d", got, want)
	}
}

func TestReserve_AfterCloseReturnsError(t *testing.T) {
	mgr, err := NewManager(tempPath(t), minChunkSize)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := mgr.Reserve(1); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestCeilChunk(t *testing.T) {
	const chunk = int64(10)
	cases := []struct {
		size, want int64
	}{
		{0, 0},
		{1, 10},
		{10, 10},
		{11, 20},
		{20, 20},
	}
	for _, tc := range cases {
		if got := ceilChunk(tc.size, chunk); got != tc.want {
			t.Errorf("ceilChunk(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}
}

func TestReserve_Concurrent(t *testing.T) {
	const chunk = 2 * 1024 * 1024
	mgr, err := NewManager(tempPath(t), chunk)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	const goroutines = 16
	const perRoutine = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perRoutine; i++ {
				if _, err := mgr.Reserve(int64((g*perRoutine + i + 1) * 64 * 1024)); err != nil {
					t.Errorf("concurrent reserve: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// The largest request was goroutines*perRoutine*64KiB = ~50 MiB; the
	// high-water mark must cover it with one chunk of headroom and be a chunk
	// multiple.
	got := mgr.Reserved()
	if got%chunk != 0 {
		t.Fatalf("reserved %d is not a chunk multiple", got)
	}
	largest := int64(goroutines*perRoutine) * 64 * 1024
	if got < largest {
		t.Fatalf("reserved %d does not cover largest request %d", got, largest)
	}
}
