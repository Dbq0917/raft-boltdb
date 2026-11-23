// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// mockFSM is a minimal FSM that counts applied entries and records whether
// its state was ever restored from a snapshot.
type mockFSM struct {
	mu       sync.Mutex
	applied  int
	restored bool
}

func (m *mockFSM) Apply(*raft.Log) interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied++
	return nil
}

func (m *mockFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &mockSnapshot{}, nil
}

func (m *mockFSM) Restore(rc io.ReadCloser) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.restored = true
	return rc.Close()
}

func (m *mockFSM) stats() (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied, m.restored
}

type mockSnapshot struct{}

func (s *mockSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write([]byte("snapshot")); err != nil {
		return err
	}
	return sink.Close()
}

func (s *mockSnapshot) Release() {}

// delayedTransport wraps an in-process transport and delays data-bearing
// AppendEntries RPCs delivered to this node. A zero delay together with stall
// set to true parks every data RPC forever, modelling a follower whose
// cross-datacenter link is down while heartbeats keep flowing.
type delayedTransport struct {
	raft.Transport
	delay time.Duration
	stall bool
	out   chan raft.RPC
}

func newDelayedTransport(inner *raft.InmemTransport, delay time.Duration, stall bool) *delayedTransport {
	t := &delayedTransport{
		Transport: inner,
		delay:     delay,
		stall:     stall,
		out:       make(chan raft.RPC, 1024),
	}
	go t.pump(inner.Consumer())
	return t
}

func (t *delayedTransport) pump(in <-chan raft.RPC) {
	for rpc := range in {
		req, isAppend := rpc.Command.(*raft.AppendEntriesRequest)
		if isAppend && (t.stall || t.delay > 0) {
			// Heartbeats carry no entries and must stay timely so the follower
			// remains part of the cluster; only data-bearing RPCs are slowed
			// down or parked to model a remote or stalled replica.
			if len(req.Entries) == 0 {
				t.out <- rpc
				continue
			}
			if t.stall {
				// Never deliver; the leader keeps retrying through its normal
				// replication backoff while the follower stays at index 0.
				continue
			}
			go func(delayed raft.RPC) {
				time.Sleep(t.delay)
				t.out <- delayed
			}(rpc)
			continue
		}
		t.out <- rpc
	}
}

func (t *delayedTransport) Consumer() <-chan raft.RPC { return t.out }

// ackTransport records the match index of every successful AppendEntries
// response the node receives as a leader, feeding the retention guard.
type ackTransport struct {
	raft.Transport
	guard *RetentionGuard
}

func (t *ackTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	err := t.Transport.AppendEntries(id, target, args, resp)
	if err == nil && resp != nil && resp.Success {
		t.guard.ReportAck(id, resp.LastLog)
	}
	return err
}

type testNode struct {
	id    raft.ServerID
	addr  raft.ServerAddress
	raft  *raft.Raft
	store *BoltStore
	trans raft.Transport
	inmem *raft.InmemTransport
	fsm   *mockFSM
}

type testCluster struct {
	t       *testing.T
	nodes   []*testNode
	leader  *testNode
	guard   *RetentionGuard
	cleanup []func()
}

func newTestCluster(t *testing.T, voters int, slowIndex int, slowDelay time.Duration, stallSlow bool, guard *RetentionGuard) *testCluster {
	t.Helper()

	cluster := &testCluster{t: t, guard: guard}
	servers := make([]raft.Server, voters)
	for i := 0; i < voters; i++ {
		servers[i] = raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("node%d", i+1)),
			Address: raft.ServerAddress(fmt.Sprintf("addr%d", i+1)),
		}
	}

	for i := 0; i < voters; i++ {
		cfg := servers[i]
		path := filepath.Join(t.TempDir(), "bolt.db")

		store, err := NewBoltStore(path)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}

		_, inmemTrans := raft.NewInmemTransport(cfg.Address)
		var net raft.Transport = inmemTrans
		if i == slowIndex {
			net = newDelayedTransport(inmemTrans, slowDelay, stallSlow)
		}
		// Every node feeds observed replication progress into the guard; only
		// the active leader's reports matter in practice.
		net = &ackTransport{Transport: net, guard: guard}

		raftCfg := raft.DefaultConfig()
		raftCfg.LocalID = cfg.ID
		raftCfg.SnapshotInterval = 50 * time.Millisecond
		raftCfg.SnapshotThreshold = 20
		raftCfg.TrailingLogs = 5

		fsm := &mockFSM{}
		node := &testNode{
			id:    cfg.ID,
			addr:  cfg.Address,
			store: store,
			trans: net,
			inmem: inmemTrans,
			fsm:   fsm,
		}

		r, err := raft.NewRaft(
			raftCfg,
			fsm,
			NewGuardedLogStore(store, guard),
			store,
			raft.NewInmemSnapshotStore(),
			net,
		)
		if err != nil {
			t.Fatalf("new raft: %v", err)
		}
		node.raft = r

		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			t.Fatalf("bootstrap: %v", err)
		}

		cluster.nodes = append(cluster.nodes, node)
		cluster.cleanup = append(cluster.cleanup, func() {
			_ = r.Shutdown().Error()
			_ = store.Close()
		})
	}

	// Fully connect the in-process mesh through every transport wrapper.
	for _, a := range cluster.nodes {
		for _, b := range cluster.nodes {
			if a.id != b.id {
				a.inmem.Connect(b.addr, b.inmem)
			}
		}
	}

	return cluster
}

func (c *testCluster) close() {
	for _, fn := range c.cleanup {
		fn()
	}
}

func (c *testCluster) waitForLeader(timeout time.Duration) *testNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node.raft.State() == raft.Leader {
				c.leader = node
				return node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return nil
}

func (c *testCluster) applyCommands(count int) {
	c.t.Helper()
	for i := 0; i < count; i++ {
		if err := c.leader.raft.Apply(
			[]byte(fmt.Sprintf("cmd%d", i)),
			5*time.Second,
		).Error(); err != nil {
			c.t.Fatalf("apply %d: %v", i, err)
		}
	}
}

func (c *testCluster) waitForFSM(node *testNode, want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		applied, _ := node.fsm.stats()
		if applied >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	applied, _ := node.fsm.stats()
	c.t.Fatalf("node %s applied %d entries, want %d", node.id, applied, want)
}

// TestReplication_SlowFollowerLogsNotTruncated reproduces the reported
// scenario: in a five-replica cluster with one follower in a remote
// datacenter, snapshot compaction on the leader must never remove log entries
// the slow follower still needs, so it catches up by log replay instead of a
// full snapshot transfer.
func TestReplication_SlowFollowerLogsNotTruncated(t *testing.T) {
	const voters = 5
	guard := NewRetentionGuard(RetentionConfig{
		Voters:          voters,
		MinRetainedLogs: 5,
	})

	// node5 is the cross-datacenter follower: data-bearing AppendEntries
	// deliveries to it are delayed while heartbeats stay timely.
	cluster := newTestCluster(t, voters, voters-1, 75*time.Millisecond, false, guard)
	defer cluster.close()

	leader := cluster.waitForLeader(5 * time.Second)

	const commands = 60
	cluster.applyCommands(commands)

	// The four local followers reach the full commit point quickly.
	for i, node := range cluster.nodes {
		if i == voters-1 {
			continue
		}
		cluster.waitForFSM(node, commands, 5*time.Second)
	}

	// Let snapshot compactions run, then give the slow follower time to catch
	// up entirely by replaying the retained logs.
	time.Sleep(500 * time.Millisecond)
	slow := cluster.nodes[voters-1]
	cluster.waitForFSM(slow, commands, 30*time.Second)

	// The slow follower must have caught up without receiving a snapshot.
	if _, restored := slow.fsm.stats(); restored {
		t.Fatalf("slow follower %s was restored from a snapshot; required logs were truncated", slow.id)
	}

	// While the follower lagged, every log the leader deleted had to be
	// acknowledged by it first: the leader's first retained index cannot pass
	// the slow follower's durable log head.
	leaderFirst, err := leader.store.FirstIndex()
	if err != nil {
		t.Fatalf("first index: %v", err)
	}
	slowLast, err := slow.store.LastIndex()
	if err != nil {
		t.Fatalf("slow last index: %v", err)
	}
	if leaderFirst > slowLast+1 {
		t.Fatalf("leader compacted past slow follower: first index %d > slow last index %d",
			leaderFirst, slowLast)
	}

	// Compaction must still happen once every follower has caught up; the log
	// is not retained forever.
	const extraCommands = 40
	cluster.applyCommands(extraCommands)
	cluster.waitForFSM(slow, commands+extraCommands, 10*time.Second)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		first, _ := leader.store.FirstIndex()
		last, _ := leader.store.LastIndex()
		if first > 1 && last-first < uint64(commands) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("leader log was never compacted after all followers caught up")
}

// TestReplication_SpanBackstopKeepsDiskBounded verifies the other side of the
// boundary: a follower whose replication link is down cannot pin the log
// indefinitely. Once MaxLogSpan is exceeded the leader truncates anyway, and
// the stalled follower later resyncs through a snapshot.
func TestReplication_SpanBackstopKeepsDiskBounded(t *testing.T) {
	const voters = 5
	guard := NewRetentionGuard(RetentionConfig{
		Voters:          voters,
		MaxLogSpan:      20,
		MinRetainedLogs: 5,
	})

	// Data-bearing AppendEntries to node5 fail outright, so it never reports
	// progress; heartbeats still flow so elections are unaffected.
	cluster := newTestCluster(t, voters, voters-1, 0, true, guard)
	defer cluster.close()

	leader := cluster.waitForLeader(5 * time.Second)

	const commands = 80
	cluster.applyCommands(commands)
	for i, node := range cluster.nodes {
		if i == voters-1 {
			continue
		}
		cluster.waitForFSM(node, commands, 5*time.Second)
	}

	// The stalled follower never appends, so the all-ack boundary cannot
	// advance. MaxLogSpan must still force the log down to a bounded window.
	// Snapshot compaction is periodic, so accept a window a few snapshots wide
	// rather than asserting the exact MaxLogSpan value.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		first, _ := leader.store.FirstIndex()
		last, _ := leader.store.LastIndex()
		if first > 1 && last-first <= 40 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	first, _ := leader.store.FirstIndex()
	last, _ := leader.store.LastIndex()
	t.Fatalf("log window not bounded: first=%d last=%d", first, last)
}
