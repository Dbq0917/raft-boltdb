// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

var (
	// ErrLogRangeUnavailable indicates that a requested range is not fully
	// present in the log store. Callers can use errors.As with
	// LogRangeUnavailableError to inspect the missing index and current bounds.
	ErrLogRangeUnavailable = errors.New("raft log range unavailable")

	// ErrInvalidLogRange indicates that a requested range is malformed.
	ErrInvalidLogRange = errors.New("invalid raft log range")
)

// LogRangeUnavailableError describes a log range that cannot be read or
// leased as a contiguous range.
type LogRangeUnavailableError struct {
	Min     uint64
	Max     uint64
	Missing uint64
	First   uint64
	Last    uint64
}

func (e *LogRangeUnavailableError) Error() string {
	return fmt.Sprintf("raft log range [%d,%d] is unavailable: missing index %d (first=%d last=%d)",
		e.Min, e.Max, e.Missing, e.First, e.Last)
}

// Is allows callers to treat an unavailable range as either a retention error
// or a missing log entry.
func (e *LogRangeUnavailableError) Is(target error) bool {
	return target == ErrLogRangeUnavailable || target == raft.ErrLogNotFound
}

func (e *LogRangeUnavailableError) Unwrap() error {
	return ErrLogRangeUnavailable
}

type logLeaseRange struct {
	min uint64
	max uint64
}

type logRetentionState struct {
	mu     sync.Mutex
	nextID uint64
	leases map[uint64]logLeaseRange
}

// protectedRanges returns the union of active leases clipped to [min,max].
// The caller must hold r.mu.
func (r *logRetentionState) protectedRanges(min, max uint64) []logLeaseRange {
	if len(r.leases) == 0 {
		return nil
	}

	ranges := make([]logLeaseRange, 0, len(r.leases))
	for _, lease := range r.leases {
		lo, hi := lease.min, lease.max
		if lo < min {
			lo = min
		}
		if hi > max {
			hi = max
		}
		if lo <= hi {
			ranges = append(ranges, logLeaseRange{min: lo, max: hi})
		}
	}
	if len(ranges) < 2 {
		return ranges
	}

	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].min == ranges[j].min {
			return ranges[i].max < ranges[j].max
		}
		return ranges[i].min < ranges[j].min
	})

	merged := ranges[:1]
	for _, current := range ranges[1:] {
		last := &merged[len(merged)-1]
		adjacent := last.max != ^uint64(0) && current.min == last.max+1
		if current.min <= last.max || adjacent {
			if current.max > last.max {
				last.max = current.max
			}
			continue
		}
		merged = append(merged, current)
	}
	return merged
}

// LogLease prevents DeleteRange from removing a contiguous range of log
// entries. It does not block compaction outside the leased range and does not
// hold a database transaction.
//
// A lease is process-local. Callers must release it only after the snapshot
// that references the range is durable, including an fsync of the snapshot
// contents and the directory entry that makes the snapshot visible. If the
// process exits while a lease is held, no leased entries will have been
// compacted, so recovery can safely retry from the same boundary.
type LogLease struct {
	store *BoltStore
	id    uint64
	min   uint64
	max   uint64
}

// Range returns the inclusive range protected by the lease.
func (l *LogLease) Range() (uint64, uint64) {
	if l == nil {
		return 0, 0
	}
	return l.min, l.max
}

// Release removes the lease. It is safe to call more than once and from
// multiple goroutines.
func (l *LogLease) Release() {
	if l == nil || l.store == nil {
		return
	}

	l.store.retention.mu.Lock()
	delete(l.store.retention.leases, l.id)
	l.store.retention.mu.Unlock()
}

// AcquireLogLease validates that [min,max] is present and protects it from
// concurrent DeleteRange calls. Validation and registration are atomic with
// respect to compaction: either the range is complete when the lease is
// returned, or AcquireLogLease returns a LogRangeUnavailableError.
//
// Snapshot generation should acquire a lease before reading entries and release
// it only after the snapshot and its directory entry are durably installed.
func (b *BoltStore) AcquireLogLease(min, max uint64) (*LogLease, error) {
	if min == 0 || min > max {
		return nil, ErrInvalidLogRange
	}

	b.retention.mu.Lock()
	defer b.retention.mu.Unlock()

	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := b.verifyLogRangeTx(tx, min, max); err != nil {
		return nil, err
	}

	if b.retention.leases == nil {
		b.retention.leases = make(map[uint64]logLeaseRange)
	}
	for {
		b.retention.nextID++
		if b.retention.nextID == 0 {
			continue
		}
		if _, exists := b.retention.leases[b.retention.nextID]; !exists {
			break
		}
	}

	id := b.retention.nextID
	b.retention.leases[id] = logLeaseRange{min: min, max: max}
	return &LogLease{store: b, id: id, min: min, max: max}, nil
}

// RetainLogs is an alias for AcquireLogLease.
func (b *BoltStore) RetainLogs(min, max uint64) (*LogLease, error) {
	return b.AcquireLogLease(min, max)
}

// VerifyLogRange checks that [min,max] is present without creating a lease.
// It is useful for recovery planning and for checking a snapshot's source
// range before reading it.
func (b *BoltStore) VerifyLogRange(min, max uint64) error {
	if min == 0 || min > max {
		return ErrInvalidLogRange
	}

	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	return b.verifyLogRangeTx(tx, min, max)
}

func (b *BoltStore) verifyLogRangeTx(tx *bolt.Tx, min, max uint64) error {
	curs := tx.Bucket(dbLogs).Cursor()
	expected := min
	for k, _ := curs.Seek(uint64ToBytes(min)); k != nil; k, _ = curs.Next() {
		idx := bytesToUint64(k)
		if idx > max {
			break
		}
		if idx != expected {
			return b.logRangeUnavailable(tx, min, max, expected)
		}
		if idx == max {
			return nil
		}
		expected++
	}

	return b.logRangeUnavailable(tx, min, max, expected)
}

func (b *BoltStore) logRangeUnavailable(tx *bolt.Tx, min, max, missing uint64) error {
	first, last := logBounds(tx)
	return &LogRangeUnavailableError{
		Min:     min,
		Max:     max,
		Missing: missing,
		First:   first,
		Last:    last,
	}
}

// ReadLogRange returns a consistent copy of [min,max]. The read transaction is
// shared by the whole range, so concurrent compaction cannot expose a partial
// range to the caller.
func (b *BoltStore) ReadLogRange(min, max uint64) ([]*raft.Log, error) {
	if min == 0 || min > max {
		return nil, ErrInvalidLogRange
	}

	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := b.verifyLogRangeTx(tx, min, max); err != nil {
		return nil, err
	}

	var logs []*raft.Log
	if span := max - min; span <= 1<<20 {
		logs = make([]*raft.Log, 0, int(span+1))
	}
	curs := tx.Bucket(dbLogs).Cursor()
	for k, v := curs.Seek(uint64ToBytes(min)); k != nil; k, v = curs.Next() {
		idx := bytesToUint64(k)
		if idx > max {
			break
		}

		log := new(raft.Log)
		if err := decodeMsgPack(v, log); err != nil {
			return nil, fmt.Errorf("decode raft log at index %d: %w", idx, err)
		}
		logs = append(logs, log)
		if idx == max {
			break
		}
	}
	return logs, nil
}

// RecoveryPlan describes how to resume from a snapshot without rebuilding the
// state machine. Indexes are inclusive; zero means that the corresponding
// operation is not needed.
type RecoveryPlan struct {
	SnapshotIndex uint64
	FirstIndex    uint64
	LastIndex     uint64
	ReplayFrom    uint64
	ReplayTo      uint64

	// MissingFrom is the first log index needed after the snapshot that the
	// store cannot provide. SnapshotRequired is true when it is set.
	MissingFrom uint64

	// MinSnapshotIndex is the oldest snapshot boundary that would bridge the
	// gap while allowing the remaining logs to be replayed.
	MinSnapshotIndex uint64
	SnapshotRequired bool
	Empty            bool
}

// Retryable reports whether recovery can be retried after installing a newer
// snapshot. Callers should not rebuild the state machine when this is true or
// when the plan is empty; they should obtain a snapshot, install it, and call
// PlanRecovery again. Retries should be bounded and use a backoff.
func (p RecoveryPlan) Retryable() bool {
	return p.SnapshotRequired || p.Empty
}

// PlanRecovery compares a snapshot boundary with the logs currently available.
// It does not acquire a lease; callers that will replay the returned range
// should acquire one immediately and retry planning if the range was compacted
// in the meantime.
func (b *BoltStore) PlanRecovery(snapshotIndex uint64) (RecoveryPlan, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return RecoveryPlan{}, err
	}
	defer func() { _ = tx.Rollback() }()

	first, last := logBounds(tx)
	plan := RecoveryPlan{
		SnapshotIndex: snapshotIndex,
		FirstIndex:    first,
		LastIndex:     last,
	}

	if last == 0 {
		plan.Empty = snapshotIndex == 0
		return plan, nil
	}
	if snapshotIndex == ^uint64(0) {
		return plan, nil
	}

	replayFrom := snapshotIndex + 1
	if snapshotIndex == 0 {
		replayFrom = 1
	}
	if last < replayFrom {
		return plan, nil
	}

	missing, next, gap := recoveryGap(tx, replayFrom, last)
	if !gap {
		plan.ReplayFrom = replayFrom
		plan.ReplayTo = last
		return plan, nil
	}

	plan.MissingFrom = missing
	plan.SnapshotRequired = true
	if next != 0 {
		plan.MinSnapshotIndex = next - 1
	} else {
		plan.MinSnapshotIndex = last
	}
	return plan, nil
}

func recoveryGap(tx *bolt.Tx, from, last uint64) (uint64, uint64, bool) {
	curs := tx.Bucket(dbLogs).Cursor()
	expected := from
	for k, _ := curs.Seek(uint64ToBytes(from)); k != nil; k, _ = curs.Next() {
		idx := bytesToUint64(k)
		if idx > last {
			break
		}
		if idx != expected {
			return expected, idx, true
		}
		if idx == last {
			return 0, 0, false
		}
		expected++
	}

	return expected, 0, true
}

func logBounds(tx *bolt.Tx) (uint64, uint64) {
	curs := tx.Bucket(dbLogs).Cursor()
	first, _ := curs.First()
	last, _ := curs.Last()

	var firstIdx, lastIdx uint64
	if first != nil {
		firstIdx = bytesToUint64(first)
	}
	if last != nil {
		lastIdx = bytesToUint64(last)
	}
	return firstIdx, lastIdx
}
