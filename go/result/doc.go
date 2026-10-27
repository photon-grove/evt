// Package result provides a Rust-style Result type for propagating success or error values. The
// repository stream contracts (StreamAllEvents, StreamEntities, SnapshotStreamer and
// RebuildProjectionsFromStream) send it over channels, so each element is either a value or an
// error.
package result
