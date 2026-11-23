## Unreleased

### Improvements

* Add an opt-in snapshot barrier (`SetSnapshotBarrier`/`GetSnapshotBarrier`) so a snapshot path and a log deletion path on separate call paths can be ordered: once a barrier is recorded, `DeleteRange` will not compact the head of the log past the last durably persisted snapshot, guaranteeing at least one copy of the data survives a crash between the two actions.

### Changes

### Fixed

### Security
