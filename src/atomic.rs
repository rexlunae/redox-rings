//! The atomics the ring protocol is built on.
//!
//! Normal builds use `core::sync::atomic`. Under `--cfg loom` the same names come from
//! [loom](https://docs.rs/loom), which runs the wait/notify handshake in `sync` through every
//! interleaving and every value the C11 memory model lets each load return. See the `loom`
//! tests in `sync::tests`; loom is a dev-dependency, so only the library's own test build
//! (`cargo test --lib`) can enable the cfg.

#[cfg(not(loom))]
pub(crate) use core::sync::atomic::{AtomicU32, Ordering, fence};
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU32, Ordering, fence};
