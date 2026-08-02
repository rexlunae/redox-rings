use core::cell::UnsafeCell;
use core::marker::PhantomData;
use core::mem;
use core::mem::MaybeUninit;
use core::ops;
use core::sync::atomic::AtomicU32;
use core::sync::atomic::Ordering;
use core::hint::unlikely;
use syscall::error::*;

#[derive(Debug, Eq, PartialEq)]
pub enum RingInitError {
    CapacityNotPowerOfTwo,
    ItemSizeMismatch,
}

impl From<RingInitError> for syscall::Error {
    fn from(value: RingInitError) -> Self {
        match value {
            RingInitError::CapacityNotPowerOfTwo => syscall::Error::new(EIO),
            RingInitError::ItemSizeMismatch => syscall::Error::new(EINVAL),
        }
    }
}

/// A wrapper that aligns a type to the size of a cache line.
///
/// [`crossbeam_utils`](https://docs.rs/crossbeam_utils) has an identical struct, but it is
/// redefined here to avoid that dependency.
#[cfg_attr(target_arch = "x86_64", repr(align(128)))]
#[cfg_attr(not(target_arch = "x86_64"), repr(align(64)))]
#[derive(Debug)]
pub struct CachePadded<T>(pub T);

impl<T> ops::Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> ops::DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct FixedParameters {
    pub ptr_queue: *mut u8,
    pub queue_len: u32,
    pub item_len: Option<core::num::NonZeroU32>,
}

pub const WAITING_BIT: u32 = 1 << 31;
pub const INDEX_MASK: u32 = !WAITING_BIT;

/// The raw data structure of the ring, shared with the producer(s) and consumer(s) using it.
#[derive(Debug)]
#[repr(C)]
pub struct RingHeader<T> {
    //
    // The ring makes heavy use of CachePadded, in order to align atomic integers to cache
    // lines. Thus, a `Ring` shall always have at least a page of space.
    //
    /// Index of the head pointer with various information encoded together with it.
    pub head: CachePadded<AtomicU32>,
    /// Index of the tail pointer with various information encoded together with it.
    pub tail: CachePadded<AtomicU32>,

    /// Makes the Rust compiler believe that we own a `*mut T`.
    pub _marker: PhantomData<*mut T>,
}

unsafe impl<T> Send for RingHeader<T> {}

impl<T> RingHeader<T> {
    const _ASSERT_ITEM_SIZE: () = {
        assert!(mem::size_of::<T>() != 0);
    };
    pub fn new(head: u32, tail: u32) -> Self {
        Self {
            head: CachePadded(AtomicU32::new(head)),
            tail: CachePadded(AtomicU32::new(tail)),
            _marker: PhantomData,
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

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct BrokenRing;

impl core::fmt::Display for BrokenRing {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ring is in an inconsistent state")
    }
}

#[derive(Debug, PartialEq)]
pub struct RingOverflow;

#[derive(Debug)]
pub struct Entries<'mem, T> {
    ptr: *mut T,
    log2_count: u32,

    _marker: PhantomData<&'mem [UnsafeCell<MaybeUninit<T>>]>,
}
impl<T> Entries<'_, T> {
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
impl<'mem, T> core::ops::Deref for Entries<'mem, T> {
    type Target = [UnsafeCell<MaybeUninit<T>>];

    fn deref(&self) -> &Self::Target {
        unsafe {
            core::slice::from_raw_parts(
                self.ptr.cast::<UnsafeCell<MaybeUninit<T>>>(),
                self.entry_count() as usize,
            )
        }
    }
}

pub enum RingAdvancePusherAreaError {
    Broken,
}

/// The errors that may occur when pushing to the back of a ring.
#[derive(Debug, Eq, PartialEq)]
pub enum RingPushError<T> {
    /// The ring had no more space for additional entries; however it may also indicate that
    /// the consumer was about to pop, although before the push.
    ///
    /// Contains the item that would otherwise have been pushed.
    Full(T),

    /// The ring had entered an inconsistent state, where the head or tail indices were out of
    /// bounds. Rather than making this unpredictable behavior even more unpredictable, by
    /// making up an index, this will error instead. There is no strict requirement that the
    /// ring must be destroyed after this; however, recovery is implementation-specific for now.
    Broken(T),
}
impl<T> From<RingPushError<T>> for Error {
    fn from(error: RingPushError<T>) -> Error {
        match error {
            RingPushError::Full(_) => Error::new(ENOSPC),
            RingPushError::Broken(_) => Error::new(EIO),
        }
    }
}
impl<T> core::fmt::Display for RingPushError<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Full(_) => write!(f, "ring is full"),
            Self::Broken(_) => write!(f, "ring is in a broken state"),
        }
    }
}

/// The errors that may occur when popping from the front of a ring.
#[derive(Debug, Eq, PartialEq)]
pub enum RingPopError {
    /// The ring was empty, however such a condition was not caused by a shutdown. It is
    /// recommended that some kind of notification mechanism be used in this case, apart from
    /// spinning.
    Empty,

    /// The ring has entered an inconsistent state.
    Broken,
}
impl From<RingPopError> for Error {
    fn from(error: RingPopError) -> Error {
        match error {
            RingPopError::Empty => Error::new(EWOULDBLOCK),
            RingPopError::Broken => Error::new(EIO),
        }
    }
}
impl core::fmt::Display for RingPopError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty { .. } => write!(f, "ring is empty"),
            Self::Broken { .. } => write!(f, "ring is in an inconsistent state"),
        }
    }
}

#[derive(Debug)]
pub struct RawRing<'ring, T, const IS_PRODUCER: bool> {
    pub header: &'ring RingHeader<T>,
    entries: Entries<'ring, T>,
    pub cached_index: u32,
}

impl<'ring, T, const IS_PRODUCER: bool> RawRing<'ring, T, IS_PRODUCER> {
    pub fn new(
        params: FixedParameters,
        header: &'ring RingHeader<T>,
    ) -> Result<Self, RingInitError> {
        if let Some(item_len) = params.item_len
            && mem::size_of::<T>() != item_len.get() as usize
        {
            return Err(RingInitError::ItemSizeMismatch);
        }

        let capacity = params.queue_len as usize;
        if !capacity.is_power_of_two() {
            return Err(RingInitError::CapacityNotPowerOfTwo);
        }

        let log2_count = capacity.trailing_zeros();

        let entries = Entries {
            ptr: params.ptr_queue.cast(),
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
}

pub type RawProducer<'ring, T> = RawRing<'ring, T, true>;
pub type RawConsumer<'ring, T> = RawRing<'ring, T, false>;

unsafe impl<'ring, T: Send> Send for RawProducer<'ring, T> {}

impl<'ring, T> RawProducer<'ring, T> {
    unsafe fn acquire_exclusive_slice<'a, U>(slice: &'a [UnsafeCell<U>]) -> &'a mut [U] {
        // SAFETY: The only possible way to obtain a mutable reference from a shared reference,
        // is via UnsafeCell::get. However, we will have to cast everything to UnsafeCell<[T]>
        // first.
        let casted_shared: *const [U] = unsafe { cast_slice(slice) };
        let as_unsafecell: &UnsafeCell<[U]> =
            unsafe { &*(casted_shared as *const UnsafeCell<[_]>) };
        unsafe { as_unsafecell.get().as_mut() }.unwrap()
    }
    /// Obtain the two separate contiguous slices of slots that can be pushed into. The first
    /// slice is logically the one that eventually will be popped first.
    ///
    /// Unless the ring is manually advanced by us, then the items represented by these two
    /// slices can only grow at the end; the start of the slices is always cached by us. Should
    /// the other side of the ring modify the head index, which it is never supposed to, then
    /// the ring may just break and produce inconsistent results, but it will not trigger UB.
    pub fn push_areas<'ctx>(
        &'ctx mut self,
        offset: u32,
    ) -> Result<[&'ctx mut [MaybeUninit<T>]; 2], BrokenRing> {
        let head_raw = self.header.head.load(Ordering::Acquire) & INDEX_MASK;
        // Since ONLY WE can update the tail index, there is no need to fetch it later again.
        let virtual_tail_raw = self.cached_index.wrapping_add(offset) & INDEX_MASK;

        let fill_count = virtual_tail_raw.wrapping_sub(head_raw);
        if unlikely(fill_count > self.entries.entry_count()) {
            return Err(BrokenRing);
        }

        let [[r1, r2], _pop_ranges] = self.entries.ranges(head_raw, virtual_tail_raw);

        // We can now have up to two contiguous areas (as is the same for other ring buffers,
        // such as `VecDeque`):
        //
        // SAFETY: The only invariant we must uphold, is for the ranges to be in bounds. This
        // is an unsafe contract from calling Entries::ranges.
        let (s1, s2) = unsafe {
            (
                self.entries.get_unchecked(r1),
                self.entries.get_unchecked(r2),
            )
        };

        // SAFETY: The Rust compiler is freely allowed to assume that any memory behind &mut A,
        // regardless of whether that A has any UnsafeCells within it, MUST NOT CHANGE except
        // by the pointer owner (with the exception of volatile reads and writes, but those are
        // really only meant for driver code). This can become a problem for us, if the kernel
        // or the other process decides to write to these.
        //
        // However, we know for a fact that only the pusher of any given ring, has page-level
        // write access to the memory occupied by the entries. Therefore, the condition is
        // upheld. We can safely acquire an exclusive slice from this (exclusive as it that
        // there is no other simultaneous reader or writer __in this program__).
        Ok(unsafe {
            [
                Self::acquire_exclusive_slice(s1),
                Self::acquire_exclusive_slice(s2),
            ]
        })
    }
    pub fn push_back(&mut self, item: T) -> Result<Option<RingOverflow>, RingPushError<T>> {
        let [a1, a2] = match self.push_areas(0) {
            Ok(areas) => areas,
            Err(BrokenRing) => return Err(RingPushError::Broken(item)),
        };

        let slot = match a1.first_mut().or(a2.first_mut()) {
            Some(slot) => slot,
            None => return Err(RingPushError::Full(item)),
        };

        *slot = MaybeUninit::new(item);

        Ok(self.advance_push_area(1))
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

unsafe impl<'ring, T: Send> Send for RawConsumer<'ring, T> {}

unsafe fn cast_slice<A, B>(a: &[A]) -> *const [B] {
    const {
        assert!(mem::size_of::<A>() == mem::size_of::<B>());
    };

    core::ptr::slice_from_raw_parts(a.as_ptr().cast::<B>(), a.len())
}

impl<'ring, T> RawConsumer<'ring, T> {
    /// Obtain the two separate contiguous slices. The first item in the first slice is
    /// logically the item that was first pushed.
    ///
    /// Unless we manually advance the ring, then those two slices are only meant to grow at
    /// the end, as the head index is cached by us. If the other side of the ring would modify
    /// the head index, which it definitely should not, the ring may break. However, while this
    /// can cause errors and inconsistent behavior, it cannot cause UB because applications
    /// should never rely on the correctness of unvalidated input data!
    pub fn pop_areas<'ctx>(&'ctx mut self) -> Result<[&'ctx [UnsafeCell<T>]; 2], BrokenRing> {
        let tail_raw = self.header.tail.load(Ordering::Acquire) & INDEX_MASK;
        let head_raw = self.cached_index & INDEX_MASK;

        let fill_count = tail_raw.wrapping_sub(head_raw);
        if unlikely(fill_count > self.entries.entry_count()) {
            return Err(BrokenRing);
        }
        let [_push_ranges, [r1, r2]] = self.entries.ranges(head_raw, tail_raw);

        Ok(unsafe {
            // SAFETY: It is an unsafe contract of Entries::ranges to return ranges that are in
            // bounds.
            let s1 = self.entries.get_unchecked(r1);
            let s2 = self.entries.get_unchecked(r2);

            // SAFETY: Because (I think?) the uninitializedness invariant only applies to
            // allocations that the compiler knows about, i.e. not arbitrary data that the
            // other side of the ring writes to, we should treat this slice as containing
            // initialized data.
            [&*cast_slice(s1), &*cast_slice(s2)]
        })
    }
    /// Advance the head index by `count` items. It is a logic error for this to exceed the
    /// number of pushed items, i.e. the number of poppable items.
    pub fn advance_pop_area(&mut self, count: usize) {
        self.cached_index += count as u32;
        self.cached_index &= INDEX_MASK;
        self.header.head.store(self.cached_index, Ordering::Release);
    }
    pub fn pop_front(&mut self) -> Result<T, RingPopError> {
        let [a1, a2] = self
            .pop_areas()
            .map_err(|BrokenRing| RingPopError::Broken)?;
        let slot = a1.first().or(a2.first()).ok_or(RingPopError::Empty)?;
        let item = unsafe { slot.get().read() };
        self.advance_pop_area(1);

        Ok(item)
    }
}
