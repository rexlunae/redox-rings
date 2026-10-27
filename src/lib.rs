//! Redox-specific system library.
#![cfg_attr(not(feature = "std"), no_std)]
#![feature(likely_unlikely)]

// The user-space rings map the header into shared memory and hand its atomics to the futex
// syscalls, which loom's atomics cannot stand in for.
#[cfg(all(loom, any(feature = "userspace", feature = "std")))]
compile_error!("loom models build with `--no-default-features --features sync`");

mod atomic;
pub mod ffi;
pub mod op;
pub mod raw;

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(feature = "userspace")]
pub mod user;
