// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"testing"

	"github.com/hashicorp/raft"
)

func TestRetentionGuard_AllPolicyWaitsForSlowestFollower(t *testing.T) {
	guard := NewRetentionGuard(RetentionConfig{Voters: 5})

	// Four fast followers and one cross-datacenter follower lagging behind.
	guard.ReportAck("f1", 100)
	guard.ReportAck("f2", 100)
	guard.ReportAck("f3", 99)
	guard.ReportAck("f4", 40)

	// The leader finished a snapshot at index 90 and wants to compact it.
	if got := guard.SafeTruncateIndex(90, 100); got != 40 {
		t.Fatalf("expected truncation to stop at slowest follower match 40, got %d", got)
	}

	// Acknowledgements are monotonic.
	guard.ReportAck("f4", 20)
	if got := guard.SafeTruncateIndex(90, 100); got != 40 {
		t.Fatalf("expected stale ack to be ignored, got %d", got)
	}

	// Once the slow follower catches up the boundary advances.
	guard.ReportAck("f4", 95)
	if got := guard.SafeTruncateIndex(90, 100); got != 90 {
		t.Fatalf("expected requested index 90 after catch-up, got %d", got)
	}
}

func TestRetentionGuard_UnreportedFollowerDoesNotBlock(t *testing.T) {
	guard := NewRetentionGuard(RetentionConfig{Voters: 5})
	guard.ReportAck("f1", 100)

	// The slow follower has never reported; it cannot pin the log because a
	// node that never appended anything must sync via snapshot anyway.
	if got := guard.SafeTruncateIndex(90, 100); got != 90 {
		t.Fatalf("expected unreported follower not to block truncation, got %d", got)
	}
}

func TestRetentionGuard_RemoveFollower(t *testing.T) {
	guard := NewRetentionGuard(RetentionConfig{Voters: 3})
	guard.ReportAck("f1", 100)
	guard.ReportAck("f2", 10)
	if got := guard.SafeTruncateIndex(90, 100); got != 10 {
		t.Fatalf("expected boundary 10, got %d", got)
	}

	guard.RemoveFollower("f2")
	if got := guard.SafeTruncateIndex(90, 100); got != 90 {
		t.Fatalf("expected removed follower to release boundary, got %d", got)
	}
}

func TestRetentionGuard_QuorumPolicy(t *testing.T) {
	guard := NewRetentionGuard(RetentionConfig{
		Voters: 5,
		Policy: AckPolicyQuorum,
	})

	// No quorum yet (need Voters/2 = 2 follower reports): nothing is bounded.
	guard.ReportAck("f1", 100)
	if got := guard.SafeTruncateIndex(90, 100); got != 90 {
		t.Fatalf("expected no bound without quorum, got %d", got)
	}

	// Two fastest followers form a quorum with the leader; their slowest match
	// is 80, so truncation stops there even though f4 is at 40.
	guard.ReportAck("f2", 80)
	guard.ReportAck("f3", 40)
	if got := guard.SafeTruncateIndex(90, 100); got != 80 {
		t.Fatalf("expected quorum boundary 80, got %d", got)
	}
}

func TestRetentionGuard_SpanBackstopProtectsDisk(t *testing.T) {
	guard := NewRetentionGuard(RetentionConfig{
		Voters:     5,
		MaxLogSpan: 50,
	})

	// The slow follower is stalled far behind and would keep the whole log.
	guard.ReportAck("f1", 200)
	guard.ReportAck("f2", 200)
	guard.ReportAck("f3", 200)
	guard.ReportAck("f4", 10)

	// Snapshot at 190, head at 200: the ack bound is 10 but the span backstop
	// forces truncation up to 200-50 = 150 to avoid exhausting disk.
	if got := guard.SafeTruncateIndex(190, 200); got != 150 {
		t.Fatalf("expected span backstop at 150, got %d", got)
	}
}

func TestRetentionGuard_MinRetainedLogs(t *testing.T) {
	guard := NewRetentionGuard(RetentionConfig{
		Voters:          5,
		MinRetainedLogs: 10,
	})
	guard.ReportAck("f1", 200)
	guard.ReportAck("f2", 200)

	// Even with everyone caught up, the newest 10 entries are retained.
	if got := guard.SafeTruncateIndex(200, 200); got != 190 {
		t.Fatalf("expected 10 retained logs, got cut at %d", got)
	}
}

func TestGuardedLogStore_ClampsDeleteRange(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()

	guard := NewRetentionGuard(RetentionConfig{Voters: 3})
	guard.ReportAck("f1", 5)

	logs := make([]*raft.Log, 0, 10)
	for idx := uint64(1); idx <= 10; idx++ {
		logs = append(logs, testRaftLog(idx, "log"))
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	guarded := NewGuardedLogStore(store, guard)

	// Requesting deletion 1..8 must be clamped to the follower boundary at 5.
	if err := guarded.DeleteRange(1, 8); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.GetLog(5, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("log 5 should be deleted, got err %v", err)
	}
	if err := store.GetLog(6, new(raft.Log)); err != nil {
		t.Fatalf("log 6 must be retained for the follower, got err %v", err)
	}

	// A range entirely above the boundary is a no-op.
	if err := guarded.DeleteRange(7, 9); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.GetLog(7, new(raft.Log)); err != nil {
		t.Fatalf("log 7 must still exist, got err %v", err)
	}

	first, err := guarded.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first != 6 {
		t.Fatalf("expected first index 6 after clamped compaction, got %d", first)
	}
	last, err := guarded.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if last != 10 {
		t.Fatalf("expected last index 10, got %d", last)
	}
}
