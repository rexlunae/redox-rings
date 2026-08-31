use super::{FutexWaitResult, WaitNotify};
use crate::raw::{
    FixedParameters, RawConsumer, RawProducer, RingHeader, RingPopError, RingPushError,
};
use crate::sync::{RawBlockingConsumer, RawBlockingProducer};
use crate::user::{DEFAULT_QUEUE_LEN, HEADER_MMAP_OFFSET};
use libredox::error::{Error, Result};
use libredox::flag::{O_CLOEXEC, O_CREAT, O_EXCL, O_RDWR};
use std::fmt;
use std::mem;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU32;
use syscall::data::TimeSpec;
use syscall::error::Error as SysError;
use syscall::error::*;

pub struct BlockingProducer<Item: 'static> {
    fd: libredox::Fd,
    shm_size: usize,
    ptr: NonNull<u8>,
    pub inner: RawBlockingProducer<'static, Item, &'static RingHeader<Item>>,
}

impl<Item: 'static + fmt::Debug> fmt::Debug for BlockingProducer<Item> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingProducer")
            // .field("fd", &self.fd) // Ignore this field
            .field("shm_size", &self.shm_size)
            .field("ptr", &self.ptr)
            .field("inner", &self.inner)
            .finish()
    }
}

unsafe impl<Item: Send> Send for BlockingProducer<Item> {}

impl<Item: 'static> BlockingProducer<Item> {
    pub fn new(path: &str, nr_entries: Option<u32>) -> Result<Self> {
        let (fd, is_creator) =
            match libredox::Fd::open(path, O_CREAT | O_RDWR | O_CLOEXEC | O_EXCL, 0) {
                Ok(fd) => (fd, true),
                Err(e) if e.errno() == libredox::errno::EEXIST => {
                    let fd = libredox::Fd::open(path, O_RDWR | O_CLOEXEC, 0)?;
                    (fd, false)
                }
                Err(e) => return Err(e),
            };

        Self::from_fd(fd, is_creator, nr_entries)
    }

    /// # Errors
    ///
    /// * `EINVAL`: `nr_entries` is not a power of two, or `Item` is a ZST.
    pub fn from_fd(fd: libredox::Fd, is_creator: bool, nr_entries: Option<u32>) -> Result<Self> {
        let item_size = size_of::<Item>();
        if item_size == 0 {
            return Err(Error::new(EINVAL));
        }

        let (shm_size, queue_len) = if is_creator {
            let nr_entries = nr_entries.unwrap_or(DEFAULT_QUEUE_LEN);
            if !nr_entries.is_power_of_two() {
                return Err(Error::new(EINVAL));
            }

            let total_size = HEADER_MMAP_OFFSET + nr_entries as usize * item_size;

            // FIXME: Why does libredox::Fd::truncate() consume the FD?
            libredox::call::ftruncate(fd.raw(), total_size)?;
            (total_size.next_multiple_of(syscall::PAGE_SIZE), nr_entries)
        } else {
            let stat = fd.stat()?;
            let total_size = stat.st_size as usize;
            let nr_entries = (total_size - HEADER_MMAP_OFFSET) / item_size;

            (
                total_size.next_multiple_of(syscall::PAGE_SIZE),
                u32::try_from(nr_entries).map_err(|_| Error::new(EINVAL))?,
            )
        };

        let map = syscall::data::Map {
            offset: 0,
            size: shm_size,
            flags: syscall::MapFlags::MAP_SHARED
                | syscall::MapFlags::PROT_WRITE
                | syscall::MapFlags::PROT_READ,
            address: 0,
        };

        let ptr_raw = unsafe { syscall::fmap(fd.raw(), &map)? as *mut u8 };
        let ptr = NonNull::new(ptr_raw).ok_or(Error::new(libredox::errno::EINVAL))?;

        let header_ptr = if is_creator {
            let header_ptr = ptr_raw as *mut RingHeader<Item>;
            unsafe {
                let initial_header = RingHeader::<Item>::new(0, 0);
                std::ptr::write(header_ptr, initial_header);
            }
            header_ptr
        } else {
            ptr_raw as *mut RingHeader<Item>
        };
        let header_ref = unsafe { &*header_ptr };

        let header_ref_static: &'static RingHeader<Item> =
            unsafe { std::mem::transmute(header_ref) };

        let parameters = FixedParameters {
            ptr_queue: unsafe { ptr_raw.add(HEADER_MMAP_OFFSET) },
            queue_len,
            item_len: Some(NonZeroU32::new(item_size as u32).expect("Item size is zero")),
        };

        let raw_producer = RawProducer::<Item>::new(parameters, header_ref_static)?;

        let blocking_producer = RawBlockingProducer::new(raw_producer, header_ref_static);

        Ok(Self {
            fd,
            shm_size,
            ptr,
            inner: blocking_producer,
        })
    }

    pub fn try_push(&mut self, item: Item) -> Result<(), RingPushError<Item>> {
        self.inner.try_push(item)
    }
    pub fn push(
        &mut self,
        item: Item,
        deadline_opt: Option<&TimeSpec>,
    ) -> Result<(), RingPushError<Item>> {
        self.inner.push(item, deadline_opt)
    }
    pub fn fd(&self) -> &libredox::Fd {
        &self.fd
    }
}

impl<Item: 'static> Drop for BlockingProducer<Item> {
    fn drop(&mut self) {
        unsafe {
            let _ = syscall::funmap(self.ptr.as_ptr() as usize, self.shm_size);
        }
    }
}

pub struct BlockingConsumer<Item: 'static> {
    fd: libredox::Fd,
    shm_size: usize,
    ptr: NonNull<u8>,
    pub inner: RawBlockingConsumer<'static, Item, &'static RingHeader<Item>>,
}

impl<Item: 'static + fmt::Debug> fmt::Debug for BlockingConsumer<Item> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingConsumer")
            // .field("fd", &self.fd) // Ignore this field
            .field("shm_size", &self.shm_size)
            .field("ptr", &self.ptr)
            .field("inner", &self.inner)
            .finish()
    }
}

unsafe impl<Item: Send> Send for BlockingConsumer<Item> {}

impl<Item: 'static> BlockingConsumer<Item> {
    pub fn new(path: &str, nr_entries: Option<u32>) -> Result<Self> {
        let (fd, is_creator) =
            match libredox::Fd::open(path, O_CREAT | O_RDWR | O_CLOEXEC | O_EXCL, 0) {
                Ok(fd) => (fd, true),
                Err(e) if e.errno() == libredox::errno::EEXIST => {
                    let fd = libredox::Fd::open(path, O_RDWR | O_CLOEXEC, 0)?;
                    (fd, false)
                }
                Err(e) => return Err(e),
            };

        Self::from_fd(fd, is_creator, nr_entries)
    }

    /// # Errors
    ///
    /// * `EINVAL`: `nr_entries` is not a power of two, or `Item` is a ZST.
    pub fn from_fd(fd: libredox::Fd, is_creator: bool, nr_entries: Option<u32>) -> Result<Self> {
        let item_size = size_of::<Item>();
        if item_size == 0 {
            return Err(Error::new(EINVAL));
        }

        let (shm_size, queue_len) = if is_creator {
            let nr_entries = nr_entries.unwrap_or(DEFAULT_QUEUE_LEN);
            if !nr_entries.is_power_of_two() {
                return Err(Error::new(EINVAL));
            }

            let total_size = HEADER_MMAP_OFFSET + nr_entries as usize * item_size;

            // FIXME: Why does libredox::Fd::truncate() consume the FD?
            libredox::call::ftruncate(fd.raw(), total_size)?;
            (total_size.next_multiple_of(syscall::PAGE_SIZE), nr_entries)
        } else {
            let stat = fd.stat()?;
            let total_size = stat.st_size as usize;
            let nr_entries = (total_size - HEADER_MMAP_OFFSET) / item_size;

            (
                total_size.next_multiple_of(syscall::PAGE_SIZE),
                u32::try_from(nr_entries).map_err(|_| Error::new(EINVAL))?,
            )
        };

        let map = syscall::data::Map {
            offset: 0,
            size: shm_size,
            flags: syscall::MapFlags::MAP_SHARED
                | syscall::MapFlags::PROT_WRITE
                | syscall::MapFlags::PROT_READ,
            address: 0,
        };

        let ptr_raw = unsafe { syscall::fmap(fd.raw(), &map)? as *mut u8 };
        let ptr = NonNull::new(ptr_raw).ok_or(Error::new(libredox::errno::EINVAL))?;

        let header_ptr = if is_creator {
            let header_ptr = ptr_raw as *mut RingHeader<Item>;
            unsafe {
                let initial_header = RingHeader::<Item>::new(0, 0);
                std::ptr::write(header_ptr, initial_header);
            }
            header_ptr
        } else {
            ptr_raw as *mut RingHeader<Item>
        };
        let header_ref = unsafe { &*header_ptr };

        let header_ref_static: &'static RingHeader<Item> =
            unsafe { std::mem::transmute(header_ref) };

        let parameters = FixedParameters {
            ptr_queue: unsafe { ptr_raw.add(HEADER_MMAP_OFFSET) },
            queue_len,
            item_len: Some(NonZeroU32::new(item_size as u32).expect("Item size is zero")),
        };

        let raw_consumer = RawConsumer::<Item>::new(parameters, header_ref_static)?;

        let blocking_consumer = RawBlockingConsumer::new(raw_consumer, header_ref_static);

        Ok(Self {
            fd,
            shm_size,
            ptr,
            inner: blocking_consumer,
        })
    }
    pub fn try_pop(&mut self) -> Result<Item, RingPopError> {
        self.inner.try_pop()
    }
    pub fn pop(&mut self, deadline_opt: Option<&TimeSpec>) -> Result<Item, RingPopError> {
        self.inner.pop(deadline_opt)
    }
    pub fn fd(&self) -> &libredox::Fd {
        &self.fd
    }
}

impl<Item: 'static> Drop for BlockingConsumer<Item> {
    fn drop(&mut self) {
        unsafe {
            let _ = syscall::funmap(self.ptr.as_ptr() as usize, self.shm_size);
        }
    }
}
impl<T> WaitNotify for RingHeader<T> {
    fn wait_on_head(&self, expected_head: u32, timeout_opt: Option<&TimeSpec>) -> FutexWaitResult {
        unsafe {
            futex_wait_ptr(
                &*self.head as *const AtomicU32 as *mut u32,
                expected_head,
                timeout_opt,
            )
        }
    }

    fn notify_on_head(&self) {
        unsafe {
            futex_wake_ptr(&*self.head as *const AtomicU32 as *mut u32, 1);
        }
    }

    fn wait_on_tail(&self, expected_tail: u32, timeout_opt: Option<&TimeSpec>) -> FutexWaitResult {
        unsafe {
            futex_wait_ptr(
                &*self.tail as *const AtomicU32 as *mut u32,
                expected_tail,
                timeout_opt,
            )
        }
    }

    fn notify_on_tail(&self) {
        unsafe {
            futex_wake_ptr(&*self.tail as *const AtomicU32 as *mut u32, 1);
        }
    }
}
impl<T> WaitNotify for &RingHeader<T> {
    fn wait_on_head(&self, expected_head: u32, timeout_opt: Option<&TimeSpec>) -> FutexWaitResult {
        (*self).wait_on_head(expected_head, timeout_opt)
    }

    fn notify_on_head(&self) {
        (*self).notify_on_head();
    }

    fn wait_on_tail(&self, expected_tail: u32, timeout_opt: Option<&TimeSpec>) -> FutexWaitResult {
        (*self).wait_on_tail(expected_tail, timeout_opt)
    }

    fn notify_on_tail(&self) {
        (*self).notify_on_tail();
    }
}

unsafe fn futex_wait_ptr(
    ptr: *mut u32,
    value: u32,
    deadline_opt: Option<&TimeSpec>,
) -> FutexWaitResult {
    let libredox_deadline: Option<libredox::data::TimeSpec> =
        deadline_opt.map(|d| libredox::data::TimeSpec {
            tv_sec: d.tv_sec as _,
            tv_nsec: d.tv_nsec as _,
        });

    let deadline_ptr: *const libredox::data::TimeSpec = match libredox_deadline.as_ref() {
        Some(d) => d as *const _,
        None => core::ptr::null(),
    };

    match SysError::demux(unsafe { redox_futex_wait_v0(ptr, value, deadline_ptr) }) {
        Ok(_) => FutexWaitResult::Waited,
        Err(e) if e.errno == EAGAIN => FutexWaitResult::Stale,
        Err(e) if e.errno == ETIMEDOUT && deadline_opt.is_some() => FutexWaitResult::TimedOut,
        Err(other) => {
            eprintln!("futex failed: {}", other.text());
            FutexWaitResult::Waited
        }
    }
}

unsafe fn futex_wake_ptr(ptr: *mut u32, n: i32) -> usize {
    SysError::demux(unsafe { redox_futex_wake_v0(ptr, n as u32) }).unwrap_or(0)
}

type RawResult = usize;
unsafe extern "C" {
    fn redox_futex_wait_v0(
        addr: *mut u32,
        val: u32,
        deadline: *const libredox::data::TimeSpec,
    ) -> RawResult;
    fn redox_futex_wake_v0(addr: *mut u32, num: u32) -> RawResult;
}
