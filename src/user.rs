use crate::raw::{
    FixedParameters, RawRing, RingHeader, RingInitError, RingPopError, RingPushError,
};
use libredox::error::{Error, Result};
use libredox::flag::{O_CLOEXEC, O_CREAT, O_EXCL, O_RDWR};
use std::fmt;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use syscall::error::*;

pub const DEFAULT_QUEUE_LEN: u32 = 64;

pub const HEADER_MMAP_OFFSET: usize = 256;

pub struct Ring<Item: 'static, const IS_PRODUCER: bool> {
    fd: libredox::Fd,
    shm_size: usize,
    ptr: NonNull<u8>,
    inner: RawRing<'static, Item, IS_PRODUCER>,
}

unsafe impl<Item: Send, const IS_PRODUCER: bool> Send for Ring<Item, IS_PRODUCER> {}

pub type Producer<Item> = Ring<Item, true>;
pub type Consumer<Item> = Ring<Item, false>;

impl<Item: 'static + fmt::Debug, const IS_PRODUCER: bool> fmt::Debug for Ring<Item, IS_PRODUCER> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = if IS_PRODUCER { "Producer" } else { "Consumer" };
        f.debug_struct(name)
            // .field("fd", &self.fd) // Ignore this field
            .field("shm_size", &self.shm_size)
            .field("ptr", &self.ptr)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<Item: 'static, const IS_PRODUCER: bool> Ring<Item, IS_PRODUCER> {
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

        let raw_producer = RawRing::<Item, IS_PRODUCER>::new(parameters, header_ref_static)?;
        Ok(Self {
            fd,
            shm_size,
            ptr,
            inner: raw_producer,
        })
    }
}

impl<Item: 'static, const IS_PRODUCER: bool> Drop for Ring<Item, IS_PRODUCER> {
    fn drop(&mut self) {
        unsafe {
            let _ = syscall::funmap(self.ptr.as_ptr() as usize, self.shm_size);
        }
    }
}

impl<Item: 'static> Producer<Item> {
    pub fn push(&mut self, item: Item) -> Result<(), RingPushError<Item>> {
        match self.inner.push_back(item) {
            // TODO: Handle ring buffer overflow.
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
impl<Item: 'static> Consumer<Item> {
    pub fn pop(&mut self) -> Result<Item, RingPopError> {
        self.inner.pop_front()
    }
}

impl From<RingInitError> for libredox::error::Error {
    fn from(value: RingInitError) -> Self {
        match value {
            RingInitError::CapacityNotPowerOfTwo => {
                libredox::error::Error::new(libredox::errno::EIO)
            }
            RingInitError::ItemSizeMismatch => libredox::error::Error::new(libredox::errno::EINVAL),
        }
    }
}

impl From<RingPopError> for libredox::error::Error {
    fn from(error: RingPopError) -> Self {
        match error {
            RingPopError::Empty => libredox::error::Error::new(libredox::errno::EWOULDBLOCK),
            RingPopError::Broken => libredox::error::Error::new(libredox::errno::EIO),
        }
    }
}
impl<T> From<RingPushError<T>> for libredox::error::Error {
    fn from(error: RingPushError<T>) -> Self {
        match error {
            RingPushError::Full(_) => libredox::error::Error::new(libredox::errno::ENOSPC),
            RingPushError::Broken(_) => libredox::error::Error::new(libredox::errno::EIO),
        }
    }
}
