// Copyright IBM Corp. 2015, 2025
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

func testBoltStore(t testing.TB) *BoltStore {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBoltStore(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestBoltStore_Implements(t *testing.T) {
	var store any = &BoltStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("BoltStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("BoltStore does not implement raft.LogStore")
	}
}

func TestBoltStore_NoSyncSyncFailureIsReturned(t *testing.T) {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	store, err := New(Options{Path: fh.Name(), NoSync: true})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer store.Close()

	syncErr := errors.New("disk full")
	store.syncFn = func() error {
		return syncErr
	}

	if err := store.StoreLog(testRaftLog(1, "log1")); !errors.Is(err, syncErr) {
		t.Fatalf("expected sync error, got: %v", err)
	}
}

func TestBoltStore_NoSyncAcknowledgedLogsSurviveReopen(t *testing.T) {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	store, err := New(Options{Path: fh.Name(), NoSync: true})
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	var syncs atomic.Int32
	store.syncFn = func() error {
		syncs.Add(1)
		return store.conn.Sync()
	}

	const logCount = 16
	for i := 1; i <= logCount; i++ {
		if err := store.StoreLog(testRaftLog(uint64(i), fmt.Sprintf("log%d", i))); err != nil {
			t.Fatalf("err: %s", err)
		}
	}
	if got := syncs.Load(); got != logCount {
		t.Fatalf("expected %d syncs, got %d", logCount, got)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	reopened, err := NewBoltStore(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer reopened.Close()

	for i := 1; i <= logCount; i++ {
		var log raft.Log
		if err := reopened.GetLog(uint64(i), &log); err != nil {
			t.Fatalf("log %d missing after reopen: %v", i, err)
		}
	}
}

func TestBoltStore_ConcurrentWritesShareSync(t *testing.T) {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	store, err := New(Options{Path: fh.Name(), NoSync: true})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer store.Close()

	const concurrentLogs = 32
	firstSync := make(chan struct{})
	releaseFirstSync := make(chan struct{})
	var syncs atomic.Int32
	store.syncFn = func() error {
		if syncs.Add(1) == 1 {
			close(firstSync)
			<-releaseFirstSync
		}
		return nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, concurrentLogs+1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- store.StoreLog(testRaftLog(1, "log1"))
	}()

	<-firstSync

	start := make(chan struct{})
	for i := 2; i <= concurrentLogs+1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs <- store.StoreLog(testRaftLog(uint64(i), fmt.Sprintf("log%d", i)))
		}(i)
	}
	close(start)

	// Let all callers reach the unbuffered write queue before the first commit
	// finishes. They must then be collected into the next shared commit.
	time.Sleep(10 * time.Millisecond)
	close(releaseFirstSync)

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	if got := syncs.Load(); got != 2 {
		t.Fatalf("expected 2 group syncs, got %d", got)
	}
}

func TestBoltStore_CloseWaitsForPendingWrite(t *testing.T) {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	store, err := New(Options{Path: fh.Name(), NoSync: true})
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	store.syncFn = func() error {
		close(syncStarted)
		<-releaseSync
		return nil
	}

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- store.StoreLog(testRaftLog(1, "log1"))
	}()
	<-syncStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- store.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the pending write completed: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	close(releaseSync)
	if err := <-writeDone; err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestBoltOptionsTimeout(t *testing.T) {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())
	options := Options{
		Path: fh.Name(),
		BoltOptions: &bolt.Options{
			Timeout: time.Second / 10,
		},
	}
	store, err := New(options)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer store.Close()
	// trying to open it again should timeout
	doneCh := make(chan error, 1)
	go func() {
		_, err := New(options)
		doneCh <- err
	}()
	select {
	case err := <-doneCh:
		if err == nil || err.Error() != "timeout" {
			t.Errorf("Expected timeout error but got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("Gave up waiting for timeout response")
	}
}

func TestBoltOptionsReadOnly(t *testing.T) {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.Remove(fh.Name())
	store, err := NewBoltStore(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}
	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	store.Close()
	options := Options{
		Path: fh.Name(),
		BoltOptions: &bolt.Options{
			Timeout:  time.Second / 10,
			ReadOnly: true,
		},
	}
	roStore, err := New(options)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer roStore.Close()
	result := new(raft.Log)
	if err := roStore.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Errorf("bad: %v", result)
	}
	// Attempt to store the log, should fail on a read-only store
	err = roStore.StoreLog(log)
	if err != bolt.ErrDatabaseReadOnly {
		t.Errorf("expecting error %v, but got %v", bolt.ErrDatabaseReadOnly, err)
	}
}

func TestNewBoltStore(t *testing.T) {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBoltStore(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the file was created
	if store.path != fh.Name() {
		t.Fatalf("unexpected file path %q", store.path)
	}
	if _, err := os.Stat(fh.Name()); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Close the store so we can open again
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure our tables were created
	db, err := bolt.Open(fh.Name(), dbFileMode, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if _, err := tx.CreateBucket([]byte(dbLogs)); err != bolt.ErrBucketExists {
		t.Fatalf("bad: %v", err)
	}
	if _, err := tx.CreateBucket([]byte(dbConf)); err != bolt.ErrBucketExists {
		t.Fatalf("bad: %v", err)
	}
}

func TestBoltStore_FirstIndex(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBoltStore_LastIndex(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBoltStore_GetLog(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestBoltStore_SetLog(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestBoltStore_SetLogs(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestBoltStore_DeleteRange(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestBoltStore_Set_Get(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestBoltStore_SetUint64_GetUint64(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}
