use core::hint::unlikely;
use core::marker::PhantomData;
use core::sync::atomic::AtomicU32;
use core::sync::atomic::Ordering;
use syscall::error::*;

use crate::raw::{
    BrokenRing, CachePadded, FixedParameters, RingInitError, RingOverflow, INDEX_MASK, WAITING_BIT,
};

/// The errors that may occur when pushing to the back of a ring.
#[derive(Debug, Eq, PartialEq)]
pub enum FfiRingPushError {
    /// The ring had no more space for additional entries; however it may also indicate that
    /// the consumer was about to pop, although before the push.
    Full,

    /// The ring had entered an inconsistent state, where the head or tail indices were out of
    /// bounds. Rather than making this unpredictable behavior even more unpredictable, by
    /// making up an index, this will error instead. There is no strict requirement that the
    /// ring must be destroyed after this; however, recovery is implementation-specific for now.
    Broken,

    /// The size of item to push is mismatches with ring parameter's item size
    ItemSizeMismatch,
}
impl From<FfiRingPushError> for Error {
    fn from(error: FfiRingPushError) -> Error {
        match error {
            FfiRingPushError::Full => Error::new(ENOSPC),
            FfiRingPushError::Broken => Error::new(EIO),
            FfiRingPushError::ItemSizeMismatch => Error::new(EINVAL),
        }
    }
}
impl core::fmt::Display for FfiRingPushError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Full => write!(f, "ring is full"),
            Self::Broken => write!(f, "ring is in a broken state"),
            Self::ItemSizeMismatch => {
                write!(f, "item is mismatch with ring parameter's item size")
            }
        }
    }
}

/// The errors that may occur when popping from the front of a ring.
#[derive(Debug, Eq, PartialEq)]
pub enum FfiRingPopError {
    /// The ring was empty, however such a condition was not caused by a shutdown. It is
    /// recommended that some kind of notification mechanism be used in this case, apart from
    /// spinning.
    Empty,

    /// The ring has entered an inconsistent state.
    Broken,

    /// The size of item to pop is mismatches with ring parameter's item size
    ItemSizeMismatch,
}
impl From<FfiRingPopError> for Error {
    fn from(error: FfiRingPopError) -> Error {
        match error {
            FfiRingPopError::Empty => Error::new(EWOULDBLOCK),
            FfiRingPopError::Broken => Error::new(EIO),
            FfiRingPopError::ItemSizeMismatch => Error::new(EINVAL),
        }
    }
}
impl core::fmt::Display for FfiRingPopError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty { .. } => write!(f, "ring is empty"),
            Self::Broken { .. } => write!(f, "ring is in an inconsistent state"),
            Self::ItemSizeMismatch { .. } => {
                write!(f, "item is mismatch with ring parameter's item size")
            }
        }
    }
}

/// The raw data structure of the ring for ffi, shared with the producer(s) and consumer(s) using it.
#[derive(Debug)]
#[repr(C)]
pub struct FfiRingHeader {
    //
    // The ring makes heavy use of CachePadded, in order to align atomic integers to cache
    // lines. Thus, a `Ring` shall always have at least a page of space.
    //
    /// Index of the head pointer with various information encoded together with it.
    pub head: CachePadded<AtomicU32>,
    /// Index of the tail pointer with various information encoded together with it.
    pub tail: CachePadded<AtomicU32>,
}

impl FfiRingHeader {
    pub fn new(head: u32, tail: u32) -> Self {
        Self {
            head: CachePadded(AtomicU32::new(head)),
            tail: CachePadded(AtomicU32::new(tail)),
        }
    }

    pub fn is_wait_head(&self) -> bool {
        self.head.load(Ordering::Relaxed) & WAITING_BIT != 0
    }
    pub fn is_wait_tail(&self) -> bool {
        self.tail.load(Ordering::Relaxed) & WAITING_BIT != 0
    }
    pub fn start_wait_head(&self) {
        let _ = self.head.fetch_or(WAITING_BIT, Ordering::Relaxed);
    }
    pub fn start_wait_tail(&self) {
        let _ = self.tail.fetch_or(WAITING_BIT, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct FfiEntries<'mem> {
    ptr: *mut u8,
    item_size: u32,
    log2_count: u32,

    _marker: PhantomData<&'mem ()>,
}
impl FfiEntries<'_> {
    fn entry_count(&self) -> u32 {
        1_u32 << self.log2_count
    }
    fn index_mask(&self) -> u32 {
        self.entry_count() - 1 & INDEX_MASK
    }
    fn ranges(&self, head_raw: u32, tail_raw: u32) -> [[core::ops::Range<usize>; 2]; 2] {
        let entry_count = self.entry_count() as usize;
        let index_mask = self.index_mask();
        let head = (head_raw & index_mask) as usize;
        let tail = (tail_raw & index_mask) as usize;

        let head_cycle = ((head_raw >> self.log2_count as usize) & 1) == 1;
        let tail_cycle = ((tail_raw >> self.log2_count as usize) & 1) == 1;
        let cycle = head_cycle ^ tail_cycle;

        // The tail index is one turn ahead of the head index. The "populated" region is
        // marked with `+` and the "writable" region is marked with `=`.
        //
        // For pushing:
        // (two ranges)     (two ranges)     (one range)
        //
        // For popping:
        // (one range)      (one ranges)     (two ranges)
        //
        // (initial)
        // Cycled:          Not Cycled:      Cycled:
        //
        // HEAD>==<TAIL     ============     ++++++++++++
        // ============     HEAD>+++++++     =======<TAIL
        // ============     ++++++++++++     ============
        // ============     =======<TAIL     HEAD>+++++++
        // ============     ============     ++++++++++++
        //
        // So if unmasked HEAD and TAIL are equal, i.e. the queue is empty, then the cycle
        // condition is false, so the ranges will be (0..count, 0..0). Otherwise, if not cycle,
        // then the ranges will be (tail..count, 0..head). If cycle, then (tail..head, N/A).

        let pusher_ranges = if cycle {
            // One range
            [tail..head, 0..0]
        } else {
            // Two ranges
            [tail..entry_count, 0..head]
        };
        let popper_ranges = if cycle {
            // Two ranges
            [head..entry_count, 0..tail]
        } else {
            // One range
            [head..tail, 0..0]
        };

        [pusher_ranges, popper_ranges]
    }
}

#[derive(Debug)]
pub struct FfiRawRing<'ring, const IS_PRODUCER: bool> {
    pub header: &'ring FfiRingHeader,
    entries: FfiEntries<'ring>,
    pub cached_index: u32,
}

impl<'ring, const IS_PRODUCER: bool> FfiRawRing<'ring, IS_PRODUCER> {
    pub fn new(
        params: FixedParameters,
        header: &'ring FfiRingHeader,
    ) -> Result<Self, RingInitError> {
        let capacity = params.queue_len as usize;
        if !capacity.is_power_of_two() {
            return Err(RingInitError::CapacityNotPowerOfTwo);
        }

        let log2_count = capacity.trailing_zeros();

        let Some(item_size) = params.item_len.map(core::num::NonZeroU32::get) else {
            return Err(RingInitError::ItemSizeMismatch);
        };

        let entries = FfiEntries {
            ptr: params.ptr_queue.cast(),
            item_size,
            log2_count,
            _marker: PhantomData,
        };

        let cached_index = if IS_PRODUCER {
            header.tail.load(Ordering::Relaxed)
        } else {
            header.head.load(Ordering::Relaxed)
        };

        Ok(Self {
            header,
            entries,
            cached_index,
        })
    }
    pub fn item_size(&self) -> usize {
        self.entries.item_size as usize
    }
}

pub type FfiRawProducer<'ring> = FfiRawRing<'ring, true>;
pub type FfiRawConsumer<'ring> = FfiRawRing<'ring, false>;

unsafe impl<'ring> Send for FfiRawProducer<'ring> {}

impl<'ring> FfiRawProducer<'ring> {
    pub fn push_areas<'ctx>(
        &'ctx mut self,
        offset: u32,
    ) -> Result<[(*mut u8, usize); 2], BrokenRing> {
        let head_raw = self.header.head.load(Ordering::Acquire) & INDEX_MASK;
        let virtual_tail_raw = self.cached_index.wrapping_add(offset) & INDEX_MASK;

        let fill_count = virtual_tail_raw.wrapping_sub(head_raw);
        if unlikely(fill_count > self.entries.entry_count()) {
            return Err(BrokenRing);
        }

        let [[r1, r2], _] = self.entries.ranges(head_raw, virtual_tail_raw);

        let get_ptr_and_len = |range: core::ops::Range<usize>| -> (*mut u8, usize) {
            if range.is_empty() {
                (core::ptr::null_mut(), 0)
            } else {
                let start_offset = range.start * self.item_size();
                let ptr = self.entries.ptr.wrapping_add(start_offset);
                (ptr, range.len())
            }
        };

        Ok([get_ptr_and_len(r1), get_ptr_and_len(r2)])
    }
    pub fn push_slots(&mut self) -> Result<*mut [u8], FfiRingPushError> {
        let [a1, a2] = match self.push_areas(0) {
            Ok(areas) => areas,
            Err(BrokenRing) => return Err(FfiRingPushError::Broken),
        };

        let slot = if a1.1 > 0 {
            a1.0
        } else if a2.1 > 0 {
            a2.0
        } else {
            return Err(FfiRingPushError::Full);
        };

        Ok(core::ptr::slice_from_raw_parts_mut(slot, self.item_size()))
    }

    /// Advance the tail index by `count` items, shrinking the push area at the back.
    ///
    /// In the unlikely event that the tail index would overflow, `Some(RingOverflow)` will be
    /// returned and the caller will be responsible for notifying any eventual poller (most
    /// likely the kernel).
    ///
    /// It is a logic error to advance by more items than maximally possible, which is
    /// determined by e.g. the size of the push area.
    pub fn advance_push_area(&mut self, count: usize) -> Option<RingOverflow> {
        let (new_last_tail, overflow) = self.cached_index.overflowing_add(count as u32);
        if new_last_tail & WAITING_BIT != 0 {
            unreachable!("This will not be occured because the memory size is limited")
        }
        self.cached_index = new_last_tail & INDEX_MASK;
        self.header.tail.store(self.cached_index, Ordering::Release);

        if overflow {
            // TODO: If we are not the kernel, are we are in the polling mode, then we need to
            // syscall the kernel to tell it that there has been an overflow, if we want to be
            // pendantic. I have marked this as a todo due to the extremely unlikelihood of
            // this happening. There must be no poll between _advancing `usize::MAX + 1` times_
            // for such an event to be lost, but it could theoretically become a problem if
            // Redox would be ported to some 32-bit architecture.
            //
            // TODO: Again, if polling, maybe we could add an additional field which the kernel
            // will set to the last value if found when polling, to further reduce the
            // probability of this. Still, it would only become a problem on 32-bit
            // architectures.
            Some(RingOverflow)
        } else {
            None
        }
    }
}

unsafe impl<'ring> Send for FfiRawConsumer<'ring> {}

impl<'ring> FfiRawConsumer<'ring> {
    pub unsafe fn pop_areas<'ctx>(
        &'ctx mut self,
        offset: u32,
    ) -> Result<[(*const u8, usize); 2], FfiRingPopError> {
        let tail_raw = self.header.tail.load(Ordering::Acquire) & INDEX_MASK;
        let virtual_head_raw = self.cached_index.wrapping_add(offset) & INDEX_MASK;

        let fill_count = tail_raw.wrapping_sub(virtual_head_raw);

        if fill_count == 0 {
            return Err(FfiRingPopError::Empty);
        }

        if unlikely(fill_count > self.entries.entry_count()) {
            return Err(FfiRingPopError::Broken);
        }

        let [_push_ranges, [r1, r2]] = self.entries.ranges(virtual_head_raw, tail_raw);

        let get_ptr_and_len = |range: core::ops::Range<usize>| -> (*const u8, usize) {
            if range.is_empty() {
                (core::ptr::null(), 0)
            } else {
                let start_offset = range.start * self.item_size();
                let ptr = self.entries.ptr.wrapping_add(start_offset);
                (ptr, range.len())
            }
        };

        Ok([get_ptr_and_len(r1), get_ptr_and_len(r2)])
    }
    /// Advance the head index by `count` items. It is a logic error for this to exceed the
    /// number of pushed items, i.e. the number of poppable items.
    pub fn advance_pop_area(&mut self, count: usize) {
        self.cached_index += count as u32;
        self.cached_index &= INDEX_MASK;
        self.header.head.store(self.cached_index, Ordering::Release);
    }
    pub fn pop_front(&mut self) -> Result<*const [u8], FfiRingPopError> {
        let [a1, _a2] = unsafe { self.pop_areas(0) }?;

        let src_ptr = a1.0;

        let slice_ptr = core::ptr::slice_from_raw_parts(src_ptr, self.item_size());

        self.advance_pop_area(1);

        Ok(slice_ptr)
    }
}
