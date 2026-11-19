// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

func storeRaftLogRange(t *testing.T, store *BoltStore, first, last uint64) {
	t.Helper()

	logs := make([]*raft.Log, 0, int(last-first+1))
	for idx := first; idx <= last; idx++ {
		logs = append(logs, testRaftLog(idx, "log"))
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("failed to store logs: %v", err)
	}
}

func requireLogPresent(t *testing.T, store *BoltStore, idx uint64) {
	t.Helper()
	if err := store.GetLog(idx, new(raft.Log)); err != nil {
		t.Fatalf("expected log %d to be present: %v", idx, err)
	}
}

func requireLogMissing(t *testing.T, store *BoltStore, idx uint64) {
	t.Helper()
	if err := store.GetLog(idx, new(raft.Log)); !errors.Is(err, raft.ErrLogNotFound) {
		t.Fatalf("expected log %d to be missing, got %v", idx, err)
	}
}

func TestBoltStore_LogLeaseAllowsPartialCompaction(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer removeTestStore(t, store)

	storeRaftLogRange(t, store, 1, 6)

	lease, err := store.AcquireLogLease(2, 4)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if min, max := lease.Range(); min != 2 || max != 4 {
		t.Fatalf("unexpected lease range [%d,%d]", min, max)
	}

	if err := store.DeleteRange(1, 6); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	for idx := uint64(2); idx <= 4; idx++ {
		requireLogPresent(t, store, idx)
	}
	for _, idx := range []uint64{1, 5, 6} {
		requireLogMissing(t, store, idx)
	}

	lease.Release()
	lease.Release()

	if err := store.DeleteRange(2, 4); err != nil {
		t.Fatalf("compaction after release failed: %v", err)
	}
	for idx := uint64(2); idx <= 4; idx++ {
		requireLogMissing(t, store, idx)
	}
}

func TestBoltStore_LogLeaseValidation(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer removeTestStore(t, store)

	storeRaftLogRange(t, store, 1, 5)
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	_, err := store.AcquireLogLease(1, 3)
	if !errors.Is(err, ErrLogRangeUnavailable) {
		t.Fatalf("expected ErrLogRangeUnavailable, got %v", err)
	}
	if !errors.Is(err, raft.ErrLogNotFound) {
		t.Fatalf("expected missing log error, got %v", err)
	}
	var rangeErr *LogRangeUnavailableError
	if !errors.As(err, &rangeErr) {
		t.Fatalf("expected LogRangeUnavailableError, got %T", err)
	}
	if rangeErr.Missing != 1 || rangeErr.First != 3 || rangeErr.Last != 5 {
		t.Fatalf("unexpected range error: %+v", rangeErr)
	}

	lease, err := store.AcquireLogLease(3, 5)
	if err != nil {
		t.Fatalf("expected range after compaction to be leasable: %v", err)
	}
	lease.Release()

	for _, tc := range []struct {
		min uint64
		max uint64
	}{
		{min: 0, max: 1},
		{min: 4, max: 3},
	} {
		if _, err := store.AcquireLogLease(tc.min, tc.max); !errors.Is(err, ErrInvalidLogRange) {
			t.Fatalf("range [%d,%d]: expected ErrInvalidLogRange, got %v", tc.min, tc.max, err)
		}
	}
}

func TestBoltStore_OverlappingLogLeases(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer removeTestStore(t, store)

	storeRaftLogRange(t, store, 1, 6)
	first, err := store.AcquireLogLease(1, 4)
	if err != nil {
		t.Fatalf("failed to acquire first lease: %v", err)
	}
	second, err := store.AcquireLogLease(3, 6)
	if err != nil {
		t.Fatalf("failed to acquire second lease: %v", err)
	}

	if err := store.DeleteRange(1, 6); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}
	for idx := uint64(1); idx <= 6; idx++ {
		requireLogPresent(t, store, idx)
	}

	first.Release()
	if err := store.DeleteRange(1, 6); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}
	requireLogMissing(t, store, 1)
	requireLogMissing(t, store, 2)
	for idx := uint64(3); idx <= 6; idx++ {
		requireLogPresent(t, store, idx)
	}

	second.Release()
}

func TestBoltStore_MultipleDisjointLogLeases(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer removeTestStore(t, store)

	storeRaftLogRange(t, store, 1, 7)
	first, err := store.AcquireLogLease(2, 3)
	if err != nil {
		t.Fatalf("failed to acquire first lease: %v", err)
	}
	second, err := store.AcquireLogLease(5, 6)
	if err != nil {
		t.Fatalf("failed to acquire second lease: %v", err)
	}
	defer first.Release()
	defer second.Release()

	if err := store.DeleteRange(1, 7); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}
	for _, idx := range []uint64{2, 3, 5, 6} {
		requireLogPresent(t, store, idx)
	}
	for _, idx := range []uint64{1, 4, 7} {
		requireLogMissing(t, store, idx)
	}
}

func TestBoltStore_ReadLogRange(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer removeTestStore(t, store)

	storeRaftLogRange(t, store, 1, 5)
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	if _, err := store.ReadLogRange(1, 3); !errors.Is(err, ErrLogRangeUnavailable) {
		t.Fatalf("expected unavailable range, got %v", err)
	}

	logs, err := store.ReadLogRange(3, 5)
	if err != nil {
		t.Fatalf("failed to read log range: %v", err)
	}
	if len(logs) != 3 {
		t.Fatalf("expected 3 logs, got %d", len(logs))
	}
	for i, log := range logs {
		if log.Index != uint64(i+3) {
			t.Fatalf("unexpected log at position %d: index %d", i, log.Index)
		}
	}
}

func TestBoltStore_RecoveryPlan(t *testing.T) {
	tests := []struct {
		name          string
		first         uint64
		last          uint64
		gap           uint64
		snapshotIndex uint64
		want          RecoveryPlan
		retryable     bool
	}{
		{
			name:          "replay suffix",
			first:         1,
			last:          10,
			snapshotIndex: 5,
			want: RecoveryPlan{
				SnapshotIndex: 5,
				FirstIndex:    1,
				LastIndex:     10,
				ReplayFrom:    6,
				ReplayTo:      10,
			},
		},
		{
			name:          "gap after snapshot",
			first:         8,
			last:          10,
			snapshotIndex: 5,
			want: RecoveryPlan{
				SnapshotIndex:    5,
				FirstIndex:       8,
				LastIndex:        10,
				MissingFrom:      6,
				MinSnapshotIndex: 7,
				SnapshotRequired: true,
			},
			retryable: true,
		},
		{
			name:          "gap inside replay range",
			first:         1,
			last:          10,
			gap:           7,
			snapshotIndex: 5,
			want: RecoveryPlan{
				SnapshotIndex:    5,
				FirstIndex:       1,
				LastIndex:        10,
				MissingFrom:      7,
				MinSnapshotIndex: 7,
				SnapshotRequired: true,
			},
			retryable: true,
		},
		{
			name:          "snapshot covers logs",
			first:         1,
			last:          10,
			snapshotIndex: 12,
			want: RecoveryPlan{
				SnapshotIndex: 12,
				FirstIndex:    1,
				LastIndex:     10,
			},
		},
		{
			name:          "no snapshot replay all",
			first:         1,
			last:          10,
			snapshotIndex: 0,
			want: RecoveryPlan{
				FirstIndex: 1,
				LastIndex:  10,
				ReplayFrom: 1,
				ReplayTo:   10,
			},
		},
		{
			name:          "missing prefix without snapshot",
			first:         5,
			last:          10,
			snapshotIndex: 0,
			want: RecoveryPlan{
				FirstIndex:       5,
				LastIndex:        10,
				MissingFrom:      1,
				MinSnapshotIndex: 4,
				SnapshotRequired: true,
			},
			retryable: true,
		},
		{
			name:          "no logs and no snapshot",
			snapshotIndex: 0,
			want: RecoveryPlan{
				Empty: true,
			},
			retryable: true,
		},
		{
			name:          "no logs with snapshot",
			snapshotIndex: 5,
			want: RecoveryPlan{
				SnapshotIndex: 5,
			},
		},
		{
			name:          "snapshot at last log",
			first:         1,
			last:          5,
			snapshotIndex: 5,
			want: RecoveryPlan{
				SnapshotIndex: 5,
				FirstIndex:    1,
				LastIndex:     5,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := testBoltStore(t)
			defer store.Close()
			defer removeTestStore(t, store)

			if tc.first != 0 {
				storeRaftLogRange(t, store, tc.first, tc.last)
			}
			if tc.gap != 0 {
				if err := store.DeleteRange(tc.gap, tc.gap); err != nil {
					t.Fatalf("failed to create log gap: %v", err)
				}
			}

			got, err := store.PlanRecovery(tc.snapshotIndex)
			if err != nil {
				t.Fatalf("failed to plan recovery: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("unexpected plan:\n got: %+v\nwant: %+v", got, tc.want)
			}
			if got.Retryable() != tc.retryable {
				t.Fatalf("Retryable() = %v, want %v", got.Retryable(), tc.retryable)
			}
		})
	}
}

func TestBoltStore_LogLeaseConcurrentCompaction(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer removeTestStore(t, store)

	storeRaftLogRange(t, store, 1, 100)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.DeleteRange(1, 100); err != nil {
				t.Errorf("compaction failed: %v", err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := store.AcquireLogLease(1, 100)
			if err != nil {
				if !errors.Is(err, ErrLogRangeUnavailable) {
					t.Errorf("failed to acquire lease: %v", err)
				}
				return
			}
			defer lease.Release()

			logs, err := store.ReadLogRange(1, 100)
			if err != nil {
				t.Errorf("lease was acquired but range was unavailable: %v", err)
				return
			}
			if len(logs) != 100 {
				t.Errorf("expected 100 logs, got %d", len(logs))
			}
		}()
	}
	wg.Wait()
}

func removeTestStore(t *testing.T, store *BoltStore) {
	t.Helper()
	if err := os.Remove(store.path); err != nil {
		t.Fatalf("failed to remove test store: %v", err)
	}
}
