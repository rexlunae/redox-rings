//! Redox-specific system library.
#![cfg_attr(not(feature = "std"), no_std)]
#![feature(likely_unlikely)]

pub mod ffi;
pub mod op;
pub mod raw;

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(feature = "userspace")]
pub mod user;
