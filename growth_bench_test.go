// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/hashicorp/raft-boltdb/prealloc"
)

const (
	// benchTargetBytes is the total amount of log payload written, matching the
	// "fill to 1 GiB" write workload.
	benchTargetBytes = 1 << 30 // 1 GiB

	// benchEntrySize is the size of an individual raft log entry.
	benchEntrySize = 32 * 1024

	// benchStallThreshold flags individual writes whose latency is large enough
	// to threaten a typical Raft election timeout.
	benchStallThreshold = 5 * time.Millisecond
)

type growthReport struct {
	entries      int
	fileSize     int64
	growthEvents int
	stalls       int
	maxLatency   time.Duration
	p99Latency   time.Duration
	maxGrowth    time.Duration
	p99Growth    time.Duration
}

func fillToGigabyte(b *testing.B, chunkSize int64) growthReport {
	b.Helper()

	path := filepath.Join(b.TempDir(), "raft-growth.db")
	store, err := New(Options{
		Path:                 path,
		PreAllocateChunkSize: chunkSize,
	})
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	defer store.Close()

	entry := testRaftLog(0, string(make([]byte, benchEntrySize)))
	total := benchTargetBytes / benchEntrySize
	latencies := make([]time.Duration, 0, total)

	previousSize := int64(0)
	growthEvents := 0
	stalls := 0
	var maxLatency time.Duration
	growthLatencies := make([]time.Duration, 0, 128)
	var maxGrowth time.Duration

	b.ResetTimer()
	for i := 0; i < total; i++ {
		entry.Index = uint64(i + 1)
		start := time.Now()
		if err := store.StoreLog(entry); err != nil {
			b.Fatalf("store log %d: %v", i+1, err)
		}
		latency := time.Since(start)
		latencies = append(latencies, latency)

		if latency > maxLatency {
			maxLatency = latency
		}
		if latency >= benchStallThreshold {
			stalls++
		}

		info, err := os.Stat(path)
		if err != nil {
			b.Fatalf("stat: %v", err)
		}
		if info.Size() != previousSize {
			growthEvents++
			growthLatencies = append(growthLatencies, latency)
			if latency > maxGrowth {
				maxGrowth = latency
			}
			previousSize = info.Size()
		}
	}
	b.StopTimer()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99 := latencies[(len(latencies)*99)/100]

	var p99Growth time.Duration
	if len(growthLatencies) > 0 {
		sort.Slice(growthLatencies, func(i, j int) bool { return growthLatencies[i] < growthLatencies[j] })
		p99Growth = growthLatencies[(len(growthLatencies)*99)/100]
	}

	info, err := os.Stat(path)
	if err != nil {
		b.Fatalf("final stat: %v", err)
	}

	return growthReport{
		entries:      total,
		fileSize:     info.Size(),
		growthEvents: growthEvents,
		stalls:       stalls,
		maxLatency:   maxLatency,
		p99Latency:   p99,
		maxGrowth:    maxGrowth,
		p99Growth:    p99Growth,
	}
}

func report(b *testing.B, name string, report growthReport) {
	b.Helper()
	fmt.Printf("\n%s: entries=%d file=%d MiB growth_events=%d stalls(>=%s)=%d write_p99=%s write_max=%s growth_p99=%s growth_max=%s\n",
		name,
		report.entries,
		report.fileSize/(1024*1024),
		report.growthEvents,
		benchStallThreshold,
		report.stalls,
		report.p99Latency,
		report.maxLatency,
		report.p99Growth,
		report.maxGrowth,
	)
}

func BenchmarkGrowth_Default(b *testing.B) {
	for n := 0; n < b.N; n++ {
		report(b, "default (doubling)", fillToGigabyte(b, 0))
	}
}

func BenchmarkGrowth_PreAllocated(b *testing.B) {
	for n := 0; n < b.N; n++ {
		report(b, "prealloc 64MiB chunks", fillToGigabyte(b, prealloc.DefaultChunkSize))
	}
}
