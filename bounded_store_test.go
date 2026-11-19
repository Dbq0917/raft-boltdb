// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func testBoundedStore(t *testing.T, opts ReclaimOptions) *BoundedStore {
	t.Helper()

	fh, err := os.CreateTemp("", "bolt-bounded")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	path := fh.Name()
	fh.Close()
	os.Remove(path)

	base, err := New(Options{Path: path, NoSync: true})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	store, err := NewBoundedStore(base, opts)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	t.Cleanup(func() {
		store.Close()
		os.Remove(path)
	})
	return store
}

func testLog(idx uint64, size int) *raft.Log {
	return &raft.Log{
		Index: idx,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  make([]byte, size),
	}
}

// waitFor polls cond until it is true or the timeout expires.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBoundedStore_RequiresPositiveCap(t *testing.T) {
	base, err := New(Options{Path: filepath.Join(t.TempDir(), "raft.db")})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer base.Close()
	if _, err := NewBoundedStore(base, ReclaimOptions{}); err == nil {
		t.Fatalf("expected error for non-positive MaxLogBytes")
	}
}

func TestBoundedStore_ReclaimKeepsNewestLogs(t *testing.T) {
	const (
		entrySize = 512
		capBytes  = 64 * 1024
		minRetain = 64
		total     = 1000
	)
	store := testBoundedStore(t, ReclaimOptions{
		MaxLogBytes:   capBytes,
		MinRetainLogs: minRetain,
	})

	for i := uint64(1); i <= total; i++ {
		if err := store.StoreLog(testLog(i, entrySize)); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	// Background reclaim must keep usage under the high watermark once
	// writes stop, and must have reclaimed the oldest entries.
	deadline := time.Now().Add(10 * time.Second)
	for store.UsedLogBytes() > store.highBytes {
		if time.Now().After(deadline) {
			first, _ := store.FirstIndex()
			last, _ := store.LastIndex()
			t.Fatalf("timeout waiting for reclaim: used=%d low=%d first=%d last=%d exhausted=%v",
				store.UsedLogBytes(), store.lowBytes, first, last, store.reclaimExhausted.Load())
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("after reclaim: used=%d low=%d high=%d", store.UsedLogBytes(), store.lowBytes, store.highBytes)

	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first == 1 {
		t.Fatalf("reclaim did not delete any entries")
	}
	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if last != total {
		t.Fatalf("last index: got %d, want %d", last, total)
	}
	if last-first+1 < minRetain {
		t.Fatalf("reclaim deleted protected entries: first=%d last=%d", first, last)
	}

	// Every entry from first to last must be readable: reclaim must not
	// lose entries or leave holes.
	var out raft.Log
	for idx := first; idx <= last; idx++ {
		if err := store.GetLog(idx, &out); err != nil {
			t.Fatalf("lost log entry %d: %s", idx, err)
		}
		if out.Index != idx {
			t.Fatalf("bad log entry: got %d, want %d", out.Index, idx)
		}
	}
}

func TestBoundedStore_DeleteRangeAccounting(t *testing.T) {
	store := testBoundedStore(t, ReclaimOptions{MaxLogBytes: 1 << 20})

	for i := uint64(1); i <= 100; i++ {
		if err := store.StoreLog(testLog(i, 256)); err != nil {
			t.Fatalf("err: %s", err)
		}
	}
	used := store.UsedLogBytes()
	if used <= 0 {
		t.Fatalf("expected positive usage, got %d", used)
	}

	if err := store.DeleteRange(1, 100); err != nil {
		t.Fatalf("err: %s", err)
	}
	if got := store.UsedLogBytes(); got != 0 {
		t.Fatalf("expected zero usage after DeleteRange, got %d", got)
	}
}

func TestBoundedStore_HardCapDoesNotDeadlock(t *testing.T) {
	const minRetain = 16
	// Cap so small that the protected entries alone nearly fill it:
	// reclaim can never get below the low watermark, and writes must
	// still make progress instead of deadlocking or losing entries.
	store := testBoundedStore(t, ReclaimOptions{
		MaxLogBytes:   minRetain * 600,
		MinRetainLogs: minRetain,
	})

	const total = 500
	for i := uint64(1); i <= total; i++ {
		if err := store.StoreLog(testLog(i, 512)); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if last != total {
		t.Fatalf("last index: got %d, want %d", last, total)
	}
	// The newest MinRetainLogs entries must survive.
	var out raft.Log
	for idx := uint64(total - minRetain + 1); idx <= total; idx++ {
		if err := store.GetLog(idx, &out); err != nil {
			t.Fatalf("lost protected log entry %d: %s", idx, err)
		}
	}
}

// latencyBucket accumulates write latencies for one fill-level band.
type latencyBucket struct {
	samples []time.Duration
}

func (b *latencyBucket) add(d time.Duration) { b.samples = append(b.samples, d) }

func (b *latencyBucket) percentile(p float64) time.Duration {
	if len(b.samples) == 0 {
		return 0
	}
	sort.Slice(b.samples, func(i, j int) bool { return b.samples[i] < b.samples[j] })
	idx := int(p * float64(len(b.samples)-1))
	return b.samples[idx]
}

func printLatencyCurve(t *testing.T, name string, bands []string, buckets map[string]*latencyBucket) {
	t.Helper()
	t.Logf("%-8s %-14s %8s %10s %10s %10s", name, "fill", "writes", "p50", "p99", "max")
	for _, band := range bands {
		b := buckets[band]
		if b == nil || len(b.samples) == 0 {
			continue
		}
		t.Logf("%-8s %-14s %8d %10s %10s %10s", name, band, len(b.samples),
			b.percentile(0.50), b.percentile(0.99), b.percentile(1.0))
	}
}

var curveBands = []string{"50-60%", "60-70%", "70-80%", "80-90%", "90-100%", ">=100%"}

func curveBand(used, capBytes int64) string {
	pct := float64(used) / float64(capBytes) * 100
	switch {
	case pct < 50:
		return ""
	case pct < 60:
		return "50-60%"
	case pct < 70:
		return "60-70%"
	case pct < 80:
		return "70-80%"
	case pct < 90:
		return "80-90%"
	case pct < 100:
		return "90-100%"
	default:
		return ">=100%"
	}
}

// TestBoundedStore_LatencyCurve pushes writes against the cap and reports
// the write-latency curve per fill level. It contrasts the old behavior
// (reclaim runs synchronously in the write path once the log hits the
// cap) with the new behavior (background reclaim starts at the high
// watermark and runs in parallel with writes).
func TestBoundedStore_LatencyCurve(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency curve in short mode")
	}

	const (
		entrySize = 1024
		capBytes  = 4 << 20 // ~4k entries
		minRetain = 256
		total     = 9000 // fill to ~100% and keep pushing at the cap
	)
	opts := ReclaimOptions{MaxLogBytes: capBytes, MinRetainLogs: minRetain}

	run := func(name string, store *BoundedStore, syncReclaimAtCap bool) map[string]*latencyBucket {
		buckets := map[string]*latencyBucket{}
		for i := uint64(1); i <= total; i++ {
			band := curveBand(store.UsedLogBytes(), capBytes)
			start := time.Now()
			if syncReclaimAtCap {
				// Old behavior: once the log hits the cap, every write
				// first pays for reclaiming a whole segment's worth of
				// entries in one synchronous transaction.
				if store.UsedLogBytes()+entrySize > capBytes {
					first, _ := store.FirstIndex()
					last, _ := store.LastIndex()
					end := first + 511
					if floor := last - minRetain + 1; end > floor {
						end = floor
					}
					if err := store.DeleteRange(first, end); err != nil {
						t.Fatalf("err: %s", err)
					}
				}
			}
			if err := store.StoreLog(testLog(i, entrySize)); err != nil {
				t.Fatalf("err: %s", err)
			}
			if band != "" {
				b := buckets[band]
				if b == nil {
					b = &latencyBucket{}
					buckets[band] = b
				}
				b.add(time.Since(start))
			}
		}
		printLatencyCurve(t, name, curveBands, buckets)
		return buckets
	}

	// Old behavior: reclaim runs synchronously in the write path once
	// the log hits the cap.
	syncStore := testBoundedStore(t, opts)
	// Stop the background reclaimer so all reclaim happens inline.
	syncStore.closed.Store(true)
	close(syncStore.done)
	syncStore.wg.Wait()
	run("sync", syncStore, true)

	// New behavior: async background reclaim kicked at the high watermark.
	asyncStore := testBoundedStore(t, opts)
	asyncBuckets := run("async", asyncStore, false)

	// Background reclaim must keep usage below the hard cap: no write
	// may ever observe a full log.
	if b := asyncBuckets[">=100%"]; b != nil && len(b.samples) > 0 {
		t.Errorf("async: %d writes observed usage at or above the cap", len(b.samples))
	}

	// New behavior with the cap saturated: MinRetainLogs protects half
	// the capacity, so reclaim cannot keep usage below the cap and the
	// hard-cap path (wait for reclaim, then proceed) is exercised.
	satStore := testBoundedStore(t, ReclaimOptions{
		MaxLogBytes:   capBytes,
		MinRetainLogs: 2100, // ~half of cap
	})
	run("async-cap", satStore, false)
}
