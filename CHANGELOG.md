## Unreleased

### Improvements

- Added snapshot-safe log leases. Snapshot readers can protect a log range
  while compaction continues deleting unprotected ranges, with atomic range
  validation and typed missing-range errors.
- Added recovery planning that distinguishes a replayable log suffix from a
  missing segment and tells callers the oldest snapshot boundary needed to
  retry recovery without rebuilding the state machine.

### Changes

### Fixed

### Security
