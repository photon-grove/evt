//! Storage and stream wire formats shared with the Go `evt` framework.
//!
//! These modules have no I/O and no SDK dependencies. The backend modules build on them, and the
//! cross-language fixtures in `compat/` test them byte for byte against the Go implementation.

pub mod dynamo;
pub mod envelope;
pub mod postgres;
