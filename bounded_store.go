// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	metrics "github.com/hashicorp/go-metrics"
	"github.com/hashicorp/raft"
)

const (
	// defaultHighWatermark is the fraction of MaxLogBytes at which
	// background reclaim starts.
	defaultHighWatermark = 0.8
	// defaultLowWatermark is the fraction of MaxLogBytes down to which
	// background reclaim frees space before going idle again.
	defaultLowWatermark = 0.6
	// defaultMinRetainLogs is the number of newest log entries that
	// reclaim never deletes.
	defaultMinRetainLogs = 128
	// defaultReclaimBatchSize is the maximum number of log entries
	// deleted in a single reclaim transaction.
	defaultReclaimBatchSize = 64
)

// ReclaimOptions configures the bounded log reclaim of a BoundedStore.
type ReclaimOptions struct {
	// MaxLogBytes is the hard cap on the estimated bytes held by the
	// raft log bucket (keys and values). It must be greater than zero.
	MaxLogBytes int64

	// HighWatermark is the fraction of MaxLogBytes at which background
	// reclaim starts. Defaults to 0.8.
	HighWatermark float64

	// LowWatermark is the fraction of MaxLogBytes down to which
	// background reclaim frees space. Defaults to 0.6.
	LowWatermark float64

	// MinRetainLogs is the number of newest log entries that reclaim
	// never deletes, so reclaim can never remove entries that may still
	// be needed. Defaults to 128.
	MinRetainLogs uint64

	// ReclaimBatchSize is the maximum number of log entries deleted in
	// a single reclaim transaction. Small batches keep reclaim
	// transactions short so they do not stall writers for long.
	// Defaults to 64.
	ReclaimBatchSize int

	// ReclaimBatchInterval is the pause between background reclaim
	// batches. While no writer is blocked at the hard cap, reclaim
	// yields to writes between batches so the two do not contend for
	// the same write transaction. When a writer is blocked at the cap,
	// reclaim runs back-to-back batches until there is space again.
	// Defaults to 1ms.
	ReclaimBatchInterval time.Duration
}

func (o *ReclaimOptions) withDefaults() error {
	if o.MaxLogBytes <= 0 {
		return errors.New("MaxLogBytes must be greater than zero")
	}
	if o.HighWatermark <= 0 || o.HighWatermark >= 1 {
		o.HighWatermark = defaultHighWatermark
	}
	if o.LowWatermark <= 0 || o.LowWatermark >= o.HighWatermark {
		o.LowWatermark = defaultLowWatermark
	}
	if o.MinRetainLogs == 0 {
		o.MinRetainLogs = defaultMinRetainLogs
	}
	if o.ReclaimBatchSize <= 0 {
		o.ReclaimBatchSize = defaultReclaimBatchSize
	}
	if o.ReclaimBatchInterval <= 0 {
		o.ReclaimBatchInterval = time.Millisecond
	}
	return nil
}

// BoundedStore wraps a BoltStore with a size cap on the raft log.
//
// Reclaim of old log entries runs in a background goroutine in parallel
// with writes: it is triggered early, once the estimated log size crosses
// the high watermark, instead of running synchronously in the write path
// after the cap has already been hit.
//
// The arbitration between reclaim and writes is defined as follows:
// writes always have priority: they never wait while the store is below
// the hard cap, and background reclaim yields to them between batches.
// Only at the hard cap does the order flip: a write waits for reclaim to
// free space first, and reclaim runs at full speed until it has.
// Reclaim never deletes the newest MinRetainLogs
// entries, so no log entry is lost because of reclaim. If the remaining
// entries are all protected by MinRetainLogs, the cap yields to
// retention and writes proceed instead of blocking forever.
type BoundedStore struct {
	*BoltStore

	opts      ReclaimOptions
	highBytes int64
	lowBytes  int64

	// usedBytes is the estimated number of bytes held by the log bucket.
	usedBytes atomic.Int64
	// reclaimExhausted is set when reclaim cannot free anything because
	// all remaining entries are protected by MinRetainLogs. It is
	// cleared whenever new logs are stored.
	reclaimExhausted atomic.Bool

	// mu and cond coordinate writes waiting for space at the hard cap.
	mu      sync.Mutex
	cond    sync.Cond
	waiters atomic.Int32

	wake   chan struct{}
	done   chan struct{}
	closed atomic.Bool
	wg     sync.WaitGroup
}

// NewBoundedStore wraps store with a bounded raft log governed by opts.
// It takes over reclaim of old log entries; raft's own DeleteRange calls
// keep working and stay accounted. The caller must use the returned
// BoundedStore for all subsequent operations, including Close.
func NewBoundedStore(store *BoltStore, opts ReclaimOptions) (*BoundedStore, error) {
	if err := opts.withDefaults(); err != nil {
		return nil, err
	}

	s := &BoundedStore{
		BoltStore: store,
		opts:      opts,
		highBytes: int64(float64(opts.MaxLogBytes) * opts.HighWatermark),
		lowBytes:  int64(float64(opts.MaxLogBytes) * opts.LowWatermark),
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	s.cond.L = &s.mu

	used, err := s.measureLogBytes()
	if err != nil {
		return nil, err
	}
	s.usedBytes.Store(used)

	s.wg.Add(1)
	go s.reclaimLoop()
	return s, nil
}

// measureLogBytes sums the key and value sizes of all stored log entries.
func (s *BoundedStore) measureLogBytes() (int64, error) {
	tx, err := s.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var total int64
	curs := tx.Bucket(dbLogs).Cursor()
	for k, v := curs.First(); k != nil; k, v = curs.Next() {
		total += int64(len(k) + len(v))
	}
	return total, nil
}

// UsedLogBytes returns the estimated number of bytes held by the log bucket.
func (s *BoundedStore) UsedLogBytes() int64 {
	return s.usedBytes.Load()
}

// StoreLog is used to store a single raft log.
func (s *BoundedStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs. Reclaim never runs
// synchronously here: at most this blocks at the hard cap until the
// background reclaimer has freed enough space.
func (s *BoundedStore) StoreLogs(logs []*raft.Log) error {
	now := time.Now()

	keys := make([][]byte, len(logs))
	vals := make([][]byte, len(logs))
	var batchBytes int64
	for i, l := range logs {
		val, err := encodeMsgPack(l)
		if err != nil {
			return err
		}
		key := uint64ToBytes(l.Index)
		keys[i] = key
		vals[i] = val.Bytes()
		batchBytes += int64(len(key) + val.Len())
		metrics.AddSample([]string{"raft", "boltdb", "logSize"}, float32(val.Len()))
	}

	s.waitForSpace(batchBytes)

	tx, err := s.conn.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	bucket := tx.Bucket(dbLogs)
	for i := range logs {
		if err := bucket.Put(keys[i], vals[i]); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	metrics.AddSample([]string{"raft", "boltdb", "logsPerBatch"}, float32(len(logs)))
	metrics.AddSample([]string{"raft", "boltdb", "logBatchSize"}, float32(batchBytes))
	metrics.MeasureSince([]string{"raft", "boltdb", "storeLogs"}, now)

	used := s.usedBytes.Add(batchBytes)
	metrics.SetGauge([]string{"raft", "boltdb", "bounded", "logBytes"}, float32(used))
	// New entries move the retention floor forward, so reclaim may be
	// able to free space again.
	s.reclaimExhausted.Store(false)

	// Space shortage is detected early: reclaim is kicked at the high
	// watermark, well before the hard cap is reached.
	if used > s.highBytes {
		s.kickReclaim()
	}
	return nil
}

// waitForSpace blocks at the hard cap until the background reclaimer has
// freed enough space for need bytes. Below the cap it returns immediately,
// giving writes priority over reclaim. If every remaining entry is
// protected by MinRetainLogs, the cap yields to retention and the write
// proceeds rather than blocking forever or losing entries.
func (s *BoundedStore) waitForSpace(need int64) {
	if s.usedBytes.Load()+need <= s.opts.MaxLogBytes {
		return
	}
	blocked := time.Now()
	blockedOnce := false

	s.mu.Lock()
	defer s.mu.Unlock()
	for s.usedBytes.Load()+need > s.opts.MaxLogBytes {
		if s.reclaimExhausted.Load() {
			break
		}
		if !blockedOnce {
			blockedOnce = true
			metrics.IncrCounter([]string{"raft", "boltdb", "bounded", "blockedWrites"}, 1)
		}
		// At the hard cap reclaim goes first: kick it and wait for it
		// to free space.
		s.waiters.Add(1)
		s.kickReclaim()
		s.cond.Wait()
		s.waiters.Add(-1)
	}
	if blockedOnce {
		metrics.AddSample([]string{"raft", "boltdb", "bounded", "blockedTime"}, float32(time.Since(blocked).Nanoseconds())/1000000)
	}
}

// kickReclaim notifies the background reclaimer without blocking.
func (s *BoundedStore) kickReclaim() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// reclaimLoop runs reclaim in the background, in parallel with writes.
func (s *BoundedStore) reclaimLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case <-s.wake:
			s.reclaim()
		}
	}
}

// reclaim deletes old entries in small batches until the estimated log
// size drops to the low watermark or only protected entries remain.
// While no writer is blocked at the hard cap it yields between batches
// so that writes always win the write transaction; when a writer is
// blocked, reclaim runs batches back-to-back.
func (s *BoundedStore) reclaim() {
	for s.usedBytes.Load() > s.lowBytes {
		freed, err := s.reclaimBatch()
		if err != nil {
			break
		}
		if freed == 0 {
			s.reclaimExhausted.Store(true)
			break
		}
		s.cond.Broadcast()
		if s.waiters.Load() == 0 {
			time.Sleep(s.opts.ReclaimBatchInterval)
		}
	}
	// Wake any waiters so they can re-evaluate, even if nothing could
	// be freed.
	s.cond.Broadcast()
}

// reclaimBatch deletes up to ReclaimBatchSize of the oldest entries in a
// single transaction. It never deletes the newest MinRetainLogs entries.
// It returns the number of entries deleted.
func (s *BoundedStore) reclaimBatch() (int, error) {
	now := time.Now()
	tx, err := s.conn.Begin(true)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	curs := tx.Bucket(dbLogs).Cursor()
	last, _ := curs.Last()
	if last == nil {
		return 0, nil
	}
	lastIdx := bytesToUint64(last)
	if lastIdx+1 <= s.opts.MinRetainLogs {
		return 0, nil
	}
	// Entries with index >= floor are protected and never deleted.
	floor := lastIdx - s.opts.MinRetainLogs + 1

	deleted := 0
	var freedBytes int64
	for k, v := curs.First(); k != nil && deleted < s.opts.ReclaimBatchSize; k, v = curs.Next() {
		if bytesToUint64(k) >= floor {
			break
		}
		freedBytes += int64(len(k) + len(v))
		if err := curs.Delete(); err != nil {
			return deleted, err
		}
		deleted++
	}
	if deleted == 0 {
		return 0, nil
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	used := s.usedBytes.Add(-freedBytes)
	metrics.SetGauge([]string{"raft", "boltdb", "bounded", "logBytes"}, float32(used))
	metrics.IncrCounter([]string{"raft", "boltdb", "bounded", "reclaimedLogs"}, float32(deleted))
	metrics.IncrCounter([]string{"raft", "boltdb", "bounded", "reclaimedBytes"}, float32(freedBytes))
	metrics.MeasureSince([]string{"raft", "boltdb", "bounded", "reclaimBatch"}, now)
	return deleted, nil
}

// DeleteRange is used to delete logs within a given range inclusively.
// It keeps the size accounting exact when raft itself compacts the log.
func (s *BoundedStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx, err := s.conn.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var freedBytes int64
	curs := tx.Bucket(dbLogs).Cursor()
	for k, v := curs.Seek(minKey); k != nil; k, v = curs.Next() {
		// Handle out-of-range log index
		if bytesToUint64(k) > max {
			break
		}
		freedBytes += int64(len(k) + len(v))

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if freedBytes > 0 {
		s.usedBytes.Add(-freedBytes)
	}
	return nil
}

// Close stops the background reclaimer and closes the underlying store.
func (s *BoundedStore) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		close(s.done)
		s.wg.Wait()
	}
	return s.BoltStore.Close()
}
