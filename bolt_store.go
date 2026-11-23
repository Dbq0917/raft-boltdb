// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"errors"
	"log"
	"time"

	"github.com/boltdb/bolt"
	metrics "github.com/hashicorp/go-metrics"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb/prealloc"
)

const (
	// Permissions to use on the db file. This is only used if the
	// database file does not exist and needs to be created.
	dbFileMode = 0600
)

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("logs")
	dbConf = []byte("conf")

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// BoltStore provides access to BoltDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type BoltStore struct {
	// conn is the underlying handle to the db.
	conn *bolt.DB

	// The path to the Bolt database file
	path string

	// preallocMgr, when non-nil, reserves file space in bounded chunks ahead
	// of writes so bolt never performs an expensive doubling grow on the
	// request path. It is nil when preallocation is disabled or in read-only
	// mode.
	preallocMgr *prealloc.Manager

	// writeWatermark tracks the cumulative logical bytes handed to the store,
	// independently of the on-disk file size, so reservations stay linear.
	writeWatermark int64
}

// Options contains all the configuration used to open the BoltDB
type Options struct {
	// Path is the file path to the BoltDB to use
	Path string

	// BoltOptions contains any specific BoltDB options you might
	// want to specify [e.g. open timeout]
	BoltOptions *bolt.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool

	// PreAllocateChunkSize controls on-demand file preallocation. When greater
	// than zero, the data file grows in fixed-size chunks reserved ahead of the
	// write position (via fallocate where supported) rather than relying on
	// bolt's doubling mmap strategy. This bounds the latency of any single
	// growth step so a large log cannot stall the write path long enough to
	// trigger a Raft election timeout. A zero value keeps bolt's default
	// behavior. Values below 1 MiB are rejected.
	PreAllocateChunkSize int64
}

// readOnly returns true if the contained bolt options say to open
// the DB in readOnly mode [this can be useful to tools that want
// to examine the log]
func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

// validatePreAllocateChunkSize rejects chunk sizes small enough that growth
// would effectively be triggered on every write.
func validatePreAllocateChunkSize(chunkSize int64) error {
	if chunkSize < prealloc.MinChunkSize() {
		return prealloc.ErrInvalidChunkSize
	}
	return nil
}

// NewBoltStore takes a file path and returns a connected Raft backend.
func NewBoltStore(path string) (*BoltStore, error) {
	return New(Options{Path: path})
}

// New uses the supplied options to open the BoltDB and prepare it for use as a raft backend.
func New(options Options) (*BoltStore, error) {
	if options.PreAllocateChunkSize > 0 {
		if err := validatePreAllocateChunkSize(options.PreAllocateChunkSize); err != nil {
			return nil, err
		}
	}

	// Try to connect
	handle, err := bolt.Open(options.Path, dbFileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	handle.NoSync = options.NoSync

	// Create the new store
	store := &BoltStore{
		conn: handle,
		path: options.Path,
	}

	// If the store was opened read-only, don't try and create buckets
	if !options.readOnly() {
		if options.PreAllocateChunkSize > 0 {
			// Use a dedicated descriptor so reservations run independently of
			// bolt's handle and only ever extend the shared inode.
			mgr, err := prealloc.NewManager(options.Path, options.PreAllocateChunkSize)
			if err != nil {
				store.Close()
				return nil, err
			}
			store.preallocMgr = mgr
		}
		// Set up our buckets
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

// initialize is used to set up all of the buckets.
func (b *BoltStore) initialize() error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Rollback failed: %v", err)
		}
	}()

	// Create all the buckets
	if _, err := tx.CreateBucketIfNotExists(dbLogs); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(dbConf); err != nil {
		return err
	}

	return tx.Commit()
}

// Close is used to gracefully close the DB connection.
func (b *BoltStore) Close() error {
	if b.preallocMgr != nil {
		// Closing the manager only releases its own descriptor; bolt's handle
		// is closed below.
		defer b.preallocMgr.Close()
	}
	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Rollback failed: %v", err)
		}
	}()

	curs := tx.Bucket(dbLogs).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

// LastIndex returns the last known index from the Raft log.
func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Rollback failed: %v", err)
		}
	}()

	curs := tx.Bucket(dbLogs).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return bytesToUint64(last), nil
	}
}

// GetLog is used to retrieve a log from BoltDB at a given index.
func (b *BoltStore) GetLog(idx uint64, raftlog *raft.Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Rollback failed: %v", err)
		}
	}()

	bucket := tx.Bucket(dbLogs)
	val := bucket.Get(uint64ToBytes(idx))

	if val == nil {
		return raft.ErrLogNotFound
	}
	return decodeMsgPack(val, raftlog)
}

// StoreLog is used to store a single raft log
func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	now := time.Now()
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Rollback failed: %v", err)
		}
	}()

	batchSize := 0
	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}

		logLen := val.Len()
		bucket := tx.Bucket(dbLogs)
		if err := bucket.Put(key, val.Bytes()); err != nil {
			return err
		}
		batchSize += logLen
		metrics.AddSample([]string{"raft", "boltdb", "logSize"}, float32(logLen))
	}

	metrics.AddSample([]string{"raft", "boltdb", "logsPerBatch"}, float32(len(logs)))
	metrics.AddSample([]string{"raft", "boltdb", "logBatchSize"}, float32(batchSize))
	// Both the deferral and the inline function are important for this metrics
	// accuracy. Deferral allows us to calculate the metric after the tx.Commit
	// has finished and thus account for all the processing of the operation.
	// The inlined function ensures that we do not calculate the time.Since(now)
	// at the time of deferral but rather when the go runtime executes the
	// deferred function.
	defer func() {
		metrics.AddSample([]string{"raft", "boltdb", "writeCapacity"}, (float32(1_000_000_000)/float32(time.Since(now).Nanoseconds()))*float32(len(logs)))
		metrics.MeasureSince([]string{"raft", "boltdb", "storeLogs"}, now)
	}()

	if b.preallocMgr != nil {
		if err := b.reserveForWrite(int64(batchSize)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// reserveForWrite makes sure the file has backing storage for the pages bolt
// is about to commit. Reservations follow the cumulative logical write
// position rather than the on-disk file size, keeping growth linear in the
// data actually written. By the time bolt grows, the target range is already
// allocated and its truncate becomes a cheap metadata operation.
func (b *BoltStore) reserveForWrite(incoming int64) error {
	b.writeWatermark += incoming
	_, err := b.preallocMgr.Reserve(b.writeWatermark)
	return err
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Rollback failed: %v", err)
		}
	}()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if bytesToUint64(k) > max {
			break
		}

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Set is used to set a key/value set outside of the raft log
func (b *BoltStore) Set(k, v []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Rollback failed: %v", err)
		}
	}()

	bucket := tx.Bucket(dbConf)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

// Get is used to retrieve a value from the k/v store by key
func (b *BoltStore) Get(k []byte) ([]byte, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Printf("Rollback failed: %v", err)
		}
	}()

	bucket := tx.Bucket(dbConf)
	val := bucket.Get(k)

	if val == nil {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), val...), nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (b *BoltStore) Sync() error {
	return b.conn.Sync()
}
