// Package result provides a Rust-style Result type for propagating success or error values.
//
// Planned for removal in the next minor release (ADR-0006, K7), once the core reshape (C1-C11)
// replaces the Stream*/SnapshotStreamer/RebuildProjectionsFromStream contracts that still
// require result.Result with plain (value, error) returns. It carries no Deprecated notice yet
// so repository implementations keep compiling warning-free.
package result
