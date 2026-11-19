raft-boltdb
===========

This repository provides the `raftboltdb` package. The package exports the
`BoltStore` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package
here](https://github.com/hashicorp/raft).

This implementation uses [BoltDB](https://github.com/boltdb/bolt). BoltDB is
a simple key/value store implemented in pure Go, and inspired by LMDB.

## Bounded Log Reclaim

`BoundedStore` wraps a `BoltStore` with a size cap on the raft log. Instead
of reclaiming space synchronously in the write path once the log is full
(which turns microsecond writes into millisecond writes at the cap), reclaim
runs in a background goroutine in parallel with writes and starts early,
once the estimated log size crosses a high watermark.

The arbitration between reclaim and writes is defined as follows:

* Writes always have priority: they never wait while the store is below the
  hard cap, and background reclaim yields to them between batches.
* Only at the hard cap does the order flip: a write waits for reclaim to
  free space first, and reclaim runs at full speed until it has.
* Reclaim never deletes the newest `MinRetainLogs` entries, so no log entry
  is lost because of reclaim. If every remaining entry is protected, the cap
  yields to retention and writes proceed instead of blocking forever.

```go
base, err := raftboltdb.New(raftboltdb.Options{Path: "raft.db"})
if err != nil {
    // handle error
}
store, err := raftboltdb.NewBoundedStore(base, raftboltdb.ReclaimOptions{
    MaxLogBytes: 1 << 30, // 1 GiB hard cap on the raft log
})
if err != nil {
    // handle error
}
// Use store as the raft LogStore/StableStore; close it instead of base.
```

## Metrics Emission and Compatibility

This library can emit metrics using either `github.com/armon/go-metrics` or `github.com/hashicorp/go-metrics`. Choosing between the libraries is controlled via build tags. 

**Build Tags**
* `armonmetrics` - Using this tag will cause metrics to be routed to `armon/go-metrics`
* `hashicorpmetrics` - Using this tag will cause all metrics to be routed to `hashicorp/go-metrics`

If no build tag is specified, the default behavior is to use `armon/go-metrics`. 

**Deprecating `armon/go-metrics`**

Emitting metrics to `armon/go-metrics` is officially deprecated. Usage of `armon/go-metrics` will remain the default until mid-2025 with opt-in support continuing to the end of 2025.

**Migration**
To migrate an application currently using the older `armon/go-metrics` to instead use `hashicorp/go-metrics` the following should be done.

1. Upgrade libraries using `armon/go-metrics` to consume `hashicorp/go-metrics/compat` instead. This should involve only changing import statements. All repositories in the `hashicorp` namespace
2. Update an applications library dependencies to those that have the compatibility layer configured.
3. Update the application to use `hashicorp/go-metrics` for configuring metrics export instead of `armon/go-metrics`
   * Replace all application imports of `github.com/armon/go-metrics` with `github.com/hashicorp/go-metrics`
   * Instrument your build system to build with the `hashicorpmetrics` tag.

Eventually once the default behavior changes to use `hashicorp/go-metrics` by default (mid-2025), you can drop the `hashicorpmetrics` build tag.

## Metrics

The following table details all the metrics emitted by this library. One note is that the application which pulls in this library may add its own prefix to the metric names. For example within [Consul](https://github.com/hashicorp/consul), the metrics will be prefixed with `consul.`.

| Metric                              | Unit         | Type    | Description           |
| ----------------------------------- | ------------:| -------:|:--------------------- |
| `raft.boltdb.freelistBytes`         | bytes        | gauge   | Represents the number of bytes necessary to encode the freelist metadata. When [`raft_boltdb.NoFreelistSync`](/docs/agent/options#NoFreelistSync) is set to `false` these metadata bytes must also be written to disk for each committed log. |
| `raft.boltdb.freePageBytes`         | bytes        | gauge   | Represents the number of bytes of free space within the raft.db file. |
| `raft.boltdb.getLog`                | ms           | timer   | Measures the amount of time spent reading logs from the db. |
| `raft.boltdb.logBatchSize`          | bytes        | sample  | Measures the total size in bytes of logs being written to the db in a single batch. |
| `raft.boltdb.logsPerBatch`          | logs         | sample  | Measures the number of logs being written per batch to the db. |
| `raft.boltdb.logSize`               | bytes        | sample  | Measures the size of logs being written to the db. |
| `raft.boltdb.numFreePages`          | pages        | gauge   | Represents the number of free pages within the raft.db file. |
| `raft.boltdb.numPendingPages`       | pages        | gauge   | Represents the number of pending pages within the raft.db that will soon become free. |
| `raft.boltdb.openReadTxn`           | transactions | gauge   | Represents the number of open read transactions against the db |
| `raft.boltdb.storeLogs`             | ms           | timer   | Measures the amount of time spent writing logs to the db. |
| `raft.boltdb.totalReadTxn`          | transactions | gauge   | Represents the total number of started read transactions against the db |
| `raft.boltdb.txstats.cursorCount`   | cursors      | counter | Counts the number of cursors created since Consul was started. |
| `raft.boltdb.txstats.nodeCount`     | allocations  | counter | Counts the number of node allocations within the db since Consul was started. |
| `raft.boltdb.txstats.nodeDeref`     | dereferences | counter | Counts the number of node dereferences in the db since Consul was started. |
| `raft.boltdb.txstats.pageAlloc`     | bytes        | gauge   | Represents the number of bytes allocated within the db since Consul was started. Note that this does not take into account space having been freed and reused. In that case, the value of this metric will still increase. |
| `raft.boltdb.txstats.pageCount`     | pages        | gauge   | Represents the number of pages allocated since Consul was started. Note that this does not take into account space having been freed and reused. In that case, the value of this metric will still increase. |
| `raft.boltdb.txstats.rebalance`     | rebalances   | counter | Counts the number of node rebalances performed in the db since Consul was started. |
| `raft.boltdb.txstats.rebalanceTime` | ms           | timer   | Measures the time spent rebalancing nodes in the db. |
| `raft.boltdb.txstats.spill`         | spills       | counter | Counts the number of nodes spilled in the db since Consul was started. |
| `raft.boltdb.txstats.spillTime`     | ms           | timer   | Measures the time spent spilling nodes in the db. |
| `raft.boltdb.txstats.split`         | splits       | counter | Counts the number of nodes split in the db since Consul was started. |
| `raft.boltdb.txstats.write`         | writes       | counter | Counts the number of writes to the db since Consul was started. |
| `raft.boltdb.txstats.writeTime`     | ms           | timer   | Measures the amount of time spent performing writes to the db. |
| `raft.boltdb.writeCapacity`         | logs/second  | sample  | Theoretical write capacity in terms of the number of logs that can be written per second. Each sample outputs what the capacity would be if future batched log write operations were similar to this one. This similarity encompasses 4 things: batch size, byte size, disk performance and boltdb performance. While none of these will be static and its highly likely individual samples of this metric will vary, aggregating this metric over a larger time window should provide a decent picture into how this BoltDB store can perform |

`BoundedStore` emits the following additional metrics:

| Metric                              | Unit         | Type    | Description           |
| ----------------------------------- | ------------:| -------:|:--------------------- |
| `raft.boltdb.bounded.logBytes`      | bytes        | gauge   | Estimated number of bytes currently held by the raft log bucket, tracked against the configured cap. |
| `raft.boltdb.bounded.blockedWrites` | writes       | counter | Counts the number of write batches that had to wait for reclaim at the hard cap. |
| `raft.boltdb.bounded.blockedTime`   | ms           | sample  | Measures how long a blocked write waited for reclaim to free space at the hard cap. |
| `raft.boltdb.bounded.reclaimedLogs` | logs         | counter | Counts the number of log entries deleted by background reclaim. |
| `raft.boltdb.bounded.reclaimedBytes`| bytes        | counter | Counts the number of bytes freed by background reclaim. |
| `raft.boltdb.bounded.reclaimBatch`  | ms           | timer   | Measures the time spent in a single background reclaim transaction. |
