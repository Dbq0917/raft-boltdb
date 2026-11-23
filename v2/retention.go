// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raftboltdb

import (
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

// RetentionConfig configures the follower-aware log retention boundary used
// by RetentionGuard.
//
// A snapshot compaction must never delete logs that a quorum of followers has
// not yet acknowledged, otherwise any lagging follower is forced to fetch a
// full snapshot. At the same time a permanently stalled follower must not be
// allowed to pin the log forever and exhaust disk. The two bounds below encode
// that trade-off: the acknowledgement bound protects followers while they are
// catching up, and the span bound is the hard disk-safety backstop.
type RetentionConfig struct {
	// Voters is the number of voting servers in the cluster, including the
	// leader. It determines how many follower acknowledgements a quorum needs
	// under AckPolicyQuorum.
	Voters int

	// Policy selects how follower acknowledgements gate compaction. The zero
	// value is AckPolicyAll, which protects every follower that has reported
	// progress and matches the default "wait for every replica" behaviour.
	Policy AckPolicy

	// MaxLogSpan bounds how far the first retained log may lag behind the last
	// log. When the span is exceeded, logs past the bound are compacted even if
	// the acknowledgement boundary would keep them. This is the disk-safety
	// backstop that prevents a permanently stalled follower from pinning the log.
	// Zero means no backstop.
	MaxLogSpan uint64

	// MinRetainedLogs is the number of newest log entries that are always kept,
	// mirroring raft's TrailingLogs setting. It is applied after every bound.
	MinRetainedLogs uint64
}

// AckPolicy selects the acknowledgement rule for the retention boundary.
type AckPolicy int

const (
	// AckPolicyAll gates compaction on the slowest follower that has reported
	// progress. Every replica stays able to catch up by log replay as long as
	// the disk span backstop is not reached. A follower that has never reported
	// does not block compaction; it is assumed to be joining or recovering from
	// a snapshot anyway.
	AckPolicyAll AckPolicy = iota

	// AckPolicyQuorum gates compaction on a quorum of replicas (the leader plus
	// Voters/2 followers). Entries acknowledged by a quorum may be compacted even
	// if one follower still needs them, trading replay availability for a smaller
	// retained log.
	AckPolicyQuorum
)

// RetentionGuard tracks the highest log index acknowledged by every follower
// and computes the highest index that may safely be compacted.
//
// The leader feeds replication progress in via ReportAck (typically derived
// from AppendEntries responses). The guard does not touch the log itself; it
// only constrains DeleteRange calls issued through a guarded store, so it can
// sit in front of any raft.LogStore implementation.
type RetentionGuard struct {
	config RetentionConfig

	mu   sync.Mutex
	acks map[raft.ServerID]uint64
}

// NewRetentionGuard creates a RetentionGuard with the given configuration.
func NewRetentionGuard(config RetentionConfig) *RetentionGuard {
	return &RetentionGuard{
		config: config,
		acks:   make(map[raft.ServerID]uint64),
	}
}

// ReportAck records that follower id has durably replicated every log entry
// up to and including matchIndex. Acknowledgements are monotonic.
func (g *RetentionGuard) ReportAck(id raft.ServerID, matchIndex uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if matchIndex > g.acks[id] {
		g.acks[id] = matchIndex
	}
}

// RemoveFollower drops the tracked progress for a follower, for example after
// it is removed from the cluster configuration.
func (g *RetentionGuard) RemoveFollower(id raft.ServerID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.acks, id)
}

// quorumBoundary returns the highest index that a quorum of followers has
// acknowledged. Logs up to this index can be compacted without forcing any
// follower that contributed to the quorum onto a snapshot.
//
// The second return value is false until enough followers have reported to
// form a quorum, in which case no quorum bound is applied yet.
func (g *RetentionGuard) quorumBoundary(lastLogIndex uint64) (uint64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// The leader's own copy always counts, so a quorum of N voters requires
	// N/2 follower acknowledgements.
	required := g.config.Voters / 2
	if required <= 0 || len(g.acks) < required {
		return 0, false
	}

	matches := make([]uint64, 0, len(g.acks))
	for _, match := range g.acks {
		matches = append(matches, match)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })

	// matches[required-1] is the slowest match among the fastest `required`
	// followers. Together with the leader those matches form a quorum, so it is
	// the highest index safely backed by a quorum of copies.
	boundary := matches[required-1]
	if boundary > lastLogIndex {
		boundary = lastLogIndex
	}
	return boundary, true
}

// allBoundary returns the highest index every follower that has reported
// progress has acknowledged, i.e. the minimum tracked match index. The second
// return value is false until at least one follower has reported.
func (g *RetentionGuard) allBoundary() (uint64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.acks) == 0 {
		return 0, false
	}
	minimum := ^uint64(0)
	for _, match := range g.acks {
		if match < minimum {
			minimum = match
		}
	}
	return minimum, true
}

// ackBoundary returns the highest index the configured policy allows to be
// compacted. A false result means no acknowledgement bound applies yet.
func (g *RetentionGuard) ackBoundary(lastLogIndex uint64) (uint64, bool) {
	switch g.config.Policy {
	case AckPolicyQuorum:
		return g.quorumBoundary(lastLogIndex)
	default:
		boundary, ok := g.allBoundary()
		if ok && boundary > lastLogIndex {
			boundary = lastLogIndex
		}
		return boundary, ok
	}
}

// SafeTruncateIndex returns the highest log index that may be deleted given
// the current follower acknowledgement state.
//
// The requested index (typically the snapshot index, optionally reduced for
// trailing logs) is clamped by two bounds:
//
//   - The acknowledgement bound: never delete beyond what the configured
//     follower set has acknowledged, so a lagging follower can catch up by log
//     replay instead of a full snapshot.
//   - The span backstop: if retaining for the followers would leave more than
//     MaxLogSpan entries, truncate down to the span bound anyway to protect
//     disk. This deliberately accepts that a permanently stalled follower
//     will need a snapshot.
func (g *RetentionGuard) SafeTruncateIndex(requested, lastLogIndex uint64) uint64 {
	if requested > lastLogIndex {
		requested = lastLogIndex
	}

	cutoff := requested
	if boundary, ok := g.ackBoundary(lastLogIndex); ok && boundary < cutoff {
		cutoff = boundary
	}

	if g.config.MaxLogSpan > 0 && lastLogIndex > g.config.MaxLogSpan {
		spanBound := lastLogIndex - g.config.MaxLogSpan
		if spanBound > cutoff {
			cutoff = spanBound
		}
	}

	if g.config.MinRetainedLogs > 0 && lastLogIndex > g.config.MinRetainedLogs {
		retainFrom := lastLogIndex - g.config.MinRetainedLogs
		if retainFrom < cutoff {
			cutoff = retainFrom
		}
	}

	return cutoff
}

// guardedLogStore wraps a raft.LogStore and routes DeleteRange through the
// RetentionGuard so that no compaction can delete logs the acknowledgement
// boundary still requires. All other LogStore methods delegate unchanged.
type guardedLogStore struct {
	raft.LogStore
	guard *RetentionGuard
}

// NewGuardedLogStore wraps store so that snapshot compactions respect the
// follower acknowledgements tracked by guard. The returned store still
// implements raft.LogStore (and any optional interfaces the wrapped store
// implements are preserved by embedding).
func NewGuardedLogStore(store raft.LogStore, guard *RetentionGuard) raft.LogStore {
	return &guardedLogStore{LogStore: store, guard: guard}
}

// DeleteRange deletes the requested range, clamping the upper bound to the
// retention boundary. A range fully above the boundary is left untouched.
func (s *guardedLogStore) DeleteRange(min, max uint64) error {
	lastLogIndex, err := s.LogStore.LastIndex()
	if err != nil {
		return err
	}

	safeMax := s.guard.SafeTruncateIndex(max, lastLogIndex)
	if safeMax < min {
		return nil
	}
	return s.LogStore.DeleteRange(min, safeMax)
}
