use crate::raw::{RawConsumer, RawProducer, RingPopError, RingPushError, INDEX_MASK, WAITING_BIT};
use core::ops::{Deref, DerefMut};
use core::sync::atomic::Ordering;
use syscall::data::TimeSpec;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FutexWaitResult {
    Waited, // possibly spurious
    Stale,  // outdated value
    TimedOut,
}

pub trait WaitNotify {
    fn wait_on_tail(&self, expected_tail: u32, deadline_opt: Option<&TimeSpec>) -> FutexWaitResult;
    fn notify_on_tail(&self);
    fn wait_on_head(&self, expected_head: u32, deadline_opt: Option<&TimeSpec>) -> FutexWaitResult;
    fn notify_on_head(&self);
}

pub trait WaitNotifyAsync {
    async fn wait_on_tail(
        &self,
        expected_tail: u32,
        deadline_opt: Option<&TimeSpec>,
    ) -> FutexWaitResult;
    fn notify_on_tail(&self);
    async fn wait_on_head(
        &self,
        expected_head: u32,
        deadline_opt: Option<&TimeSpec>,
    ) -> FutexWaitResult;
    fn notify_on_head(&self);
}

#[derive(Debug)]
pub struct RawBlockingProducer<'a, T, W: WaitNotify> {
    pub inner: RawProducer<'a, T>,
    pub waiter: W,
}

unsafe impl<'a, T: Send, W: WaitNotify> Send for RawBlockingProducer<'a, T, W> {}

impl<'a, T, W: WaitNotify> RawBlockingProducer<'a, T, W> {
    pub fn new(inner: RawProducer<'a, T>, waiter: W) -> Self {
        Self { inner, waiter }
    }

    pub fn try_push(&mut self, item: T) -> Result<(), RingPushError<T>> {
        self.inner.try_push_notify(item, &self.waiter)
    }

    pub fn push(
        &mut self,
        item: T,
        deadline_opt: Option<&TimeSpec>,
    ) -> Result<(), RingPushError<T>> {
        self.inner.push_sync(item, &self.waiter, deadline_opt)
    }

    pub fn into_inner(self) -> RawProducer<'a, T> {
        self.inner
    }
}

impl<'a, T, W: WaitNotify> Deref for RawBlockingProducer<'a, T, W> {
    type Target = RawProducer<'a, T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T, W: WaitNotify> DerefMut for RawBlockingProducer<'a, T, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Debug)]
pub struct RawBlockingConsumer<'a, T, W: WaitNotify> {
    pub inner: RawConsumer<'a, T>,
    pub waiter: W,
}

unsafe impl<'a, T: Send, W: WaitNotify> Send for RawBlockingConsumer<'a, T, W> {}

impl<'a, T, W: WaitNotify> RawBlockingConsumer<'a, T, W> {
    pub fn new(inner: RawConsumer<'a, T>, waiter: W) -> Self {
        Self { inner, waiter }
    }

    pub fn try_pop(&mut self) -> Result<T, RingPopError> {
        self.inner.try_pop_notify(&self.waiter)
    }
    pub fn pop(&mut self, deadline_opt: Option<&TimeSpec>) -> Result<T, RingPopError> {
        self.inner.pop_sync(&self.waiter, deadline_opt)
    }
}

impl<'a, T, W: WaitNotify> Deref for RawBlockingConsumer<'a, T, W> {
    type Target = RawConsumer<'a, T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T, W: WaitNotify> DerefMut for RawBlockingConsumer<'a, T, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

// TODO: How many spins should we do before it becomes more time-economical to enter kernel mode
// via futexes?
pub const SPIN_COUNT: usize = 100;

impl<'ring, T> RawProducer<'ring, T> {
    pub fn try_push_notify<W: WaitNotify>(
        &mut self,
        item: T,
        waiter: &W,
    ) -> Result<(), RingPushError<T>> {
        match self.push_back(item) {
            // TODO: Handle ring buffer overflow.
            Ok(_) => {
                // Success. Notify any waiting Consumer.
                if self.header.is_wait_head() {
                    waiter.notify_on_tail();
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    pub fn push_sync<W: WaitNotify>(
        &mut self,
        mut item: T,
        waiter: &W,
        deadline_opt: Option<&TimeSpec>,
    ) -> Result<(), RingPushError<T>> {
        'outer: loop {
            let current_head_raw = self.header.head.load(Ordering::Relaxed);
            let current_head = current_head_raw & INDEX_MASK;

            match self.push_back(item) {
                // TODO: Handle ring buffer overflow.
                Ok(_) => {
                    // Success. Notify any waiting Consumer.
                    if self.header.is_wait_head() {
                        waiter.notify_on_tail();
                    }
                    return Ok(());
                }
                Err(RingPushError::Broken(item)) => {
                    return Err(RingPushError::Broken(item));
                }
                Err(RingPushError::Full(returned_item)) => {
                    // Full. We must wait for the Consumer to pop.
                    item = returned_item;

                    let current_tail_logical = self.cached_index & INDEX_MASK;
                    let tail_with_flag = current_tail_logical | WAITING_BIT;

                    // spin SPIN_COUNT times.
                    for _ in 0..SPIN_COUNT {
                        let fresh_head = self.header.head.load(Ordering::Acquire) & INDEX_MASK;
                        if fresh_head != current_head {
                            self.header
                                .tail
                                .store(current_tail_logical, Ordering::Relaxed);
                            continue 'outer;
                        }
                        core::hint::spin_loop();
                    }
                    self.header.tail.store(tail_with_flag, Ordering::Release);

                    let fresh_head = self.header.head.load(Ordering::Acquire) & INDEX_MASK;
                    if fresh_head != current_head {
                        self.header
                            .tail
                            .store(current_tail_logical, Ordering::Relaxed);
                        continue;
                    }

                    match waiter.wait_on_head(current_head, deadline_opt) {
                        FutexWaitResult::TimedOut => {
                            return Err(RingPushError::Full(item));
                        }
                        FutexWaitResult::Waited | FutexWaitResult::Stale => {
                            // Woke up or value changed (Stale).
                            // Loop again to retry push.
                            continue;
                        }
                    }
                }
            }
        }
    }
    pub async fn push_async<W: WaitNotifyAsync>(
        &mut self,
        mut item: T,
        waiter: &W,
        deadline_opt: Option<&TimeSpec>,
    ) -> Result<(), RingPushError<T>> {
        'outer: loop {
            let current_head_raw = self.header.head.load(Ordering::Relaxed);
            let current_head = current_head_raw & INDEX_MASK;

            match self.push_back(item) {
                // TODO: Handle ring buffer overflow.
                Ok(_) => {
                    // Success. Notify any waiting Consumer.
                    if self.header.is_wait_head() {
                        waiter.notify_on_tail();
                    }
                    return Ok(());
                }
                Err(RingPushError::Broken(item)) => {
                    return Err(RingPushError::Broken(item));
                }
                Err(RingPushError::Full(returned_item)) => {
                    // Full. We must wait for the Consumer to pop.
                    item = returned_item;

                    let current_tail_logical = self.cached_index & INDEX_MASK;
                    let tail_with_flag = current_tail_logical | WAITING_BIT;

                    // spin SPIN_COUNT times.
                    for _ in 0..SPIN_COUNT {
                        let fresh_head = self.header.head.load(Ordering::Acquire) & INDEX_MASK;
                        if fresh_head != current_head {
                            self.header
                                .tail
                                .store(current_tail_logical, Ordering::Relaxed);
                            continue 'outer;
                        }
                        core::hint::spin_loop();
                    }
                    self.header.tail.store(tail_with_flag, Ordering::Release);

                    let fresh_head = self.header.head.load(Ordering::Acquire) & INDEX_MASK;
                    if fresh_head != current_head {
                        self.header
                            .tail
                            .store(current_tail_logical, Ordering::Relaxed);
                        continue;
                    }

                    match waiter.wait_on_head(current_head, deadline_opt).await {
                        FutexWaitResult::TimedOut => {
                            return Err(RingPushError::Full(item));
                        }
                        FutexWaitResult::Waited | FutexWaitResult::Stale => {
                            // Woke up or value changed (Stale).
                            // Loop again to retry push.
                            continue;
                        }
                    }
                }
            }
        }
    }
}

impl<'ring, T> RawConsumer<'ring, T> {
    pub fn try_pop_notify<W: WaitNotify>(&mut self, waiter: &W) -> Result<T, RingPopError> {
        match self.pop_front() {
            Ok(item) => {
                // Success. Notify any waiting Producer.
                if self.header.is_wait_tail() {
                    waiter.notify_on_head();
                }
                Ok(item)
            }
            Err(e) => Err(e),
        }
    }
    pub fn pop_sync<W: WaitNotify>(
        &mut self,
        waiter: &W,
        deadline_opt: Option<&TimeSpec>,
    ) -> Result<T, RingPopError> {
        'outer: loop {
            let current_tail_raw = self.header.tail.load(Ordering::Relaxed);
            let current_tail = current_tail_raw & INDEX_MASK;

            match self.pop_front() {
                Ok(item) => {
                    if self.header.is_wait_tail() {
                        waiter.notify_on_head();
                    }
                    return Ok(item);
                }
                Err(RingPopError::Broken) => {
                    return Err(RingPopError::Broken);
                }
                Err(RingPopError::Empty) => {
                    let current_head_logical = self.cached_index & INDEX_MASK;
                    let head_with_flag = current_head_logical | WAITING_BIT;

                    // spin SPIN_COUNT times.
                    for _ in 0..SPIN_COUNT {
                        let fresh_tail = self.header.tail.load(Ordering::Acquire) & INDEX_MASK;
                        if fresh_tail != current_tail {
                            self.header
                                .head
                                .store(current_head_logical, Ordering::Relaxed);
                            continue 'outer;
                        }
                        core::hint::spin_loop();
                    }
                    self.header.head.store(head_with_flag, Ordering::Release);

                    let fresh_tail = self.header.tail.load(Ordering::Acquire) & INDEX_MASK;
                    if fresh_tail != current_tail {
                        self.header
                            .head
                            .store(current_head_logical, Ordering::Relaxed);
                        continue;
                    }

                    match waiter.wait_on_tail(current_tail, deadline_opt) {
                        FutexWaitResult::TimedOut => {
                            return Err(RingPopError::Empty);
                        }
                        FutexWaitResult::Waited | FutexWaitResult::Stale => {
                            // Woke up or value changed (Stale).
                            // Loop again to retry pop.
                            continue;
                        }
                    }
                }
            }
        }
    }

    pub async fn pop_async<W: WaitNotifyAsync>(
        &mut self,
        waiter: &W,
        deadline_opt: Option<&TimeSpec>,
    ) -> Result<T, RingPopError> {
        'outer: loop {
            let current_tail_raw = self.header.tail.load(Ordering::Relaxed);
            let current_tail = current_tail_raw & INDEX_MASK;

            match self.pop_front() {
                Ok(item) => {
                    if self.header.is_wait_tail() {
                        waiter.notify_on_head();
                    }
                    return Ok(item);
                }
                Err(RingPopError::Broken) => {
                    return Err(RingPopError::Broken);
                }
                Err(RingPopError::Empty) => {
                    let current_head_logical = self.cached_index & INDEX_MASK;
                    let head_with_flag = current_head_logical | WAITING_BIT;

                    // spin SPIN_COUNT times.
                    for _ in 0..SPIN_COUNT {
                        let fresh_tail = self.header.tail.load(Ordering::Acquire) & INDEX_MASK;
                        if fresh_tail != current_tail {
                            self.header
                                .head
                                .store(current_head_logical, Ordering::Relaxed);
                            continue 'outer;
                        }
                        core::hint::spin_loop();
                    }
                    self.header.head.store(head_with_flag, Ordering::Release);

                    let fresh_tail = self.header.tail.load(Ordering::Acquire) & INDEX_MASK;
                    if fresh_tail != current_tail {
                        self.header
                            .head
                            .store(current_head_logical, Ordering::Relaxed);
                        continue;
                    }

                    match waiter.wait_on_tail(current_tail, deadline_opt).await {
                        FutexWaitResult::TimedOut => {
                            return Err(RingPopError::Empty);
                        }
                        FutexWaitResult::Waited | FutexWaitResult::Stale => {
                            // Woke up or value changed (Stale).
                            // Loop again to retry pop.
                            continue;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(feature = "userspace")]
pub mod user;
#[cfg(feature = "userspace")]
pub use user::*;
