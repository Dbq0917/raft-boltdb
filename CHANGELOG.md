## Unreleased

### Improvements

### Changes

### Fixed

- Store log, stable store, and delete operations are now acknowledged only
  after their write transaction has been durably synced. Concurrent writes
  share one commit and sync so durability does not require one `fsync` per
  caller. `NoSync` now defers the sync to the grouped commit instead of
  allowing acknowledged writes to be lost.

### Security
