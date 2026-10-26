//! Regression tests for the wait/notify handshake in this module.
//!
//! A side that finds the ring empty (or full) stores its waiting bit into its own index, fences,
//! re-reads the other index and sleeps; the other side stores its index, fences in
//! `is_wait_head`/`is_wait_tail` and wakes the sleeper only if it sees the bit. Without both
//! fences each side's load can miss the other's store (the store-buffer pattern), the notifier
//! skips the wake-up and the waiter sleeps with an item (or a free slot) already in the ring.
//!
//! Two mock waiters stand in for the real ones:
//!
//! - [`Kind::Futex`] compares the index with the expected value under its lock before sleeping,
//!   as `futex_wait` does in the kernel, so it returns `Stale` when the index has moved. This is
//!   what `RingHeader`'s own `WaitNotify` implementation uses (`pop_sync`/`push_sync`).
//! - [`Kind::Event`] queues one event per notification and has no value check, like the
//!   event-queue waiters that driver-block's ring worker passes to `pop_async`.
//!
//! The stress tests race one notification against the other side going to sleep, on real
//! threads, and count the waits that would never have ended: the condition is already true, the
//! notifier has finished, and no wake-up is coming. The `loom` models run the same handshakes
//! through every interleaving the C11 memory model allows:
//!
//! ```sh
//! cargo +nightly test --no-default-features --features sync
//! RUSTFLAGS="--cfg loom" cargo +nightly test --release --no-default-features --features sync
//! ```

extern crate std;

use super::{FutexWaitResult, WaitNotify, WaitNotifyAsync};
use crate::atomic::{AtomicU32, Ordering};
#[cfg(not(loom))]
use crate::raw::INDEX_MASK;
use crate::raw::{FixedParameters, RawConsumer, RawProducer, RingHeader};
use core::cell::UnsafeCell;
use core::future::Future;
use core::mem::MaybeUninit;
use core::num::NonZeroU32;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::boxed::Box;
use syscall::data::TimeSpec;

#[cfg(loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
use std::sync::{Condvar, Mutex, MutexGuard};

/// How a mock waiter decides whether to sleep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    /// Sleep only while the index still equals the expected value (`futex_wait`).
    Futex,
    /// Sleep until a notification is queued, whatever the index holds (an event queue).
    Event,
}

/// Which side of the ring is waiting in a test.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Waiting {
    /// The consumer waits on the tail for an item.
    Consumer,
    /// The producer waits on the head for a free slot.
    Producer,
}

#[derive(Default)]
struct Events {
    on_tail: usize,
    on_head: usize,
}

/// One ring, its slots and the mock waiter both sides share.
struct Fixture {
    header: RingHeader<u64>,
    slots: Box<[UnsafeCell<MaybeUninit<u64>>]>,
    kind: Kind,
    events: Mutex<Events>,
    wake: Condvar,
    #[cfg(not(loom))]
    probe: stress::Probe,
}

// SAFETY: `RingHeader` is `!Sync` only through its `PhantomData<*mut T>`; its fields are atomics.
// The slots are written by the single producer and read by the single consumer, in the order the
// ring's indices give, which is the protocol under test.
unsafe impl Sync for Fixture {}

impl Fixture {
    fn new(capacity: u32, kind: Kind, waiting: Waiting) -> Self {
        #[cfg(loom)]
        let _ = waiting;
        Self {
            header: RingHeader::new(0, 0),
            slots: (0..capacity)
                .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
                .collect(),
            kind,
            events: Mutex::new(Events::default()),
            wake: Condvar::new(),
            #[cfg(not(loom))]
            probe: stress::Probe::new(waiting),
        }
    }

    fn params(&self) -> FixedParameters {
        FixedParameters {
            ptr_queue: self.slots.as_ptr().cast_mut().cast(),
            queue_len: u32::try_from(self.slots.len()).expect("test rings are small"),
            item_len: NonZeroU32::new(u32::try_from(size_of::<u64>()).expect("u64 fits")),
        }
    }

    fn producer(&self) -> RawProducer<'_, u64> {
        RawProducer::new(self.params(), &self.header).expect("valid ring parameters")
    }

    fn consumer(&self) -> RawConsumer<'_, u64> {
        RawConsumer::new(self.params(), &self.header).expect("valid ring parameters")
    }

    fn waiter(&self) -> Waiter<'_> {
        Waiter(self)
    }

    fn lock(&self) -> MutexGuard<'_, Events> {
        self.events
            .lock()
            .expect("no test thread panics while holding the lock")
    }

    /// Sleep the way `kind` does: `word` is the index a futex would compare, `pending` the
    /// queued notifications an event waiter consumes.
    fn wait(
        &self,
        word: &AtomicU32,
        expected: u32,
        pending: fn(&mut Events) -> &mut usize,
    ) -> FutexWaitResult {
        let mut events = self.lock();
        match self.kind {
            Kind::Futex => {
                // The kernel compares the whole word, waiting bit included, under the lock
                // `notify` takes, so a store the notifier made before waking cannot be missed.
                if word.load(Ordering::SeqCst) != expected {
                    return FutexWaitResult::Stale;
                }
                let _events = self.sleep(events, pending);
                FutexWaitResult::Waited
            }
            Kind::Event => {
                while *pending(&mut events) == 0 {
                    events = self.sleep(events, pending);
                    #[cfg(not(loom))]
                    if self.probe.recovered() {
                        return FutexWaitResult::Waited;
                    }
                }
                *pending(&mut events) -= 1;
                FutexWaitResult::Waited
            }
        }
    }

    #[cfg(loom)]
    fn sleep<'a>(
        &'a self,
        events: MutexGuard<'a, Events>,
        _pending: fn(&mut Events) -> &mut usize,
    ) -> MutexGuard<'a, Events> {
        self.wake
            .wait(events)
            .expect("no test thread panics while holding the lock")
    }

    #[cfg(not(loom))]
    fn sleep<'a>(
        &'a self,
        events: MutexGuard<'a, Events>,
        pending: fn(&mut Events) -> &mut usize,
    ) -> MutexGuard<'a, Events> {
        let capacity = u32::try_from(self.slots.len()).expect("test rings are small");
        self.probe
            .sleep(&self.header, capacity, &self.wake, events, pending)
    }

    fn notify(&self, pending: fn(&mut Events) -> &mut usize) {
        let mut events = self.lock();
        *pending(&mut events) += 1;
        self.wake.notify_all();
    }
}

fn on_tail(events: &mut Events) -> &mut usize {
    &mut events.on_tail
}

fn on_head(events: &mut Events) -> &mut usize {
    &mut events.on_head
}

struct Waiter<'a>(&'a Fixture);

impl WaitNotify for Waiter<'_> {
    fn wait_on_tail(&self, expected: u32, _deadline: Option<&TimeSpec>) -> FutexWaitResult {
        self.0.wait(&self.0.header.tail, expected, on_tail)
    }
    fn notify_on_tail(&self) {
        self.0.notify(on_tail);
    }
    fn wait_on_head(&self, expected: u32, _deadline: Option<&TimeSpec>) -> FutexWaitResult {
        self.0.wait(&self.0.header.head, expected, on_head)
    }
    fn notify_on_head(&self) {
        self.0.notify(on_head);
    }
}

// The mocks block the calling thread instead of returning `Pending`; the protocol does not
// depend on how the waiting task is parked.
impl WaitNotifyAsync for Waiter<'_> {
    async fn wait_on_tail(&self, expected: u32, deadline: Option<&TimeSpec>) -> FutexWaitResult {
        WaitNotify::wait_on_tail(self, expected, deadline)
    }
    fn notify_on_tail(&self) {
        WaitNotify::notify_on_tail(self);
    }
    async fn wait_on_head(&self, expected: u32, deadline: Option<&TimeSpec>) -> FutexWaitResult {
        WaitNotify::wait_on_head(self, expected, deadline)
    }
    fn notify_on_head(&self) {
        WaitNotify::notify_on_head(self);
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => unreachable!("the mock waiters block instead of returning Pending"),
    }
}

/// Pop one item the way the consumer under test waits: `pop_sync` with a futex waiter,
/// `pop_async` with an event waiter.
fn pop_waiting(fixture: &Fixture, consumer: &mut RawConsumer<'_, u64>) -> u64 {
    let waiter = fixture.waiter();
    let popped = match fixture.kind {
        Kind::Futex => consumer.pop_sync(&waiter, None),
        Kind::Event => block_on(consumer.pop_async(&waiter, None)),
    };
    popped.expect("an untimed pop returns an item")
}

/// Push one item the way the producer under test waits for space.
fn push_waiting(fixture: &Fixture, producer: &mut RawProducer<'_, u64>, item: u64) {
    let waiter = fixture.waiter();
    let pushed = match fixture.kind {
        Kind::Futex => producer.push_sync(item, &waiter, None),
        Kind::Event => block_on(producer.push_async(item, &waiter, None)),
    };
    pushed.expect("an untimed push succeeds");
}

/// A push that never waits, then wakes a waiting consumer: what redoxfs's disk ring and
/// driver-block's completion pushes do.
fn push_notify(fixture: &Fixture, producer: &mut RawProducer<'_, u64>, item: u64) {
    producer
        .try_push_notify(item, &fixture.waiter())
        .expect("the test ring has room");
}

fn pop_notify(fixture: &Fixture, consumer: &mut RawConsumer<'_, u64>) -> u64 {
    consumer
        .try_pop_notify(&fixture.waiter())
        .expect("the test ring holds an item")
}

#[cfg(not(loom))]
#[test]
fn pop_async_can_succeed_without_a_notification() {
    // An item pushed before the consumer sets its bit is popped without any wake-up being sent
    // or consumed, so a caller must not assume one notification per successful `pop_async`
    // (driver-block used to do a blocking pipe read after every one).
    let fixture = Fixture::new(4, Kind::Event, Waiting::Consumer);
    let (mut producer, mut consumer) = (fixture.producer(), fixture.consumer());
    push_notify(&fixture, &mut producer, 7);
    assert_eq!(pop_waiting(&fixture, &mut consumer), 7);
    let events = fixture.lock();
    assert_eq!((events.on_tail, events.on_head), (0, 0));
}

#[cfg(not(loom))]
#[test]
fn notifier_sees_the_waiting_bit() {
    let fixture = Fixture::new(4, Kind::Event, Waiting::Consumer);
    assert!(!fixture.header.is_wait_head());
    fixture.header.start_wait_head();
    assert!(fixture.header.is_wait_head());
    // Popping stores the consumer's cached index, which clears the bit.
    let (mut producer, mut consumer) = (fixture.producer(), fixture.consumer());
    push_notify(&fixture, &mut producer, 1);
    assert_eq!(fixture.lock().on_tail, 1);
    assert_eq!(consumer.pop_front(), Ok(1));
    assert!(!fixture.header.is_wait_head());
    assert_eq!(fixture.header.head.load(Ordering::Relaxed) & !INDEX_MASK, 0);
}

#[cfg(not(loom))]
mod stress {
    //! Real-thread races. Each round lines the notifier up against the waiter beginning to
    //! sleep, with a random delay that sweeps across the waiter's spin, bit store and re-check.

    use super::*;
    use std::env;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as StdOrdering};
    use std::thread;
    use std::time::{Duration, Instant};

    /// How long a sleep lasts before the probe checks whether it is stuck.
    const SLICE: Duration = Duration::from_millis(5);
    /// A sleep this long is a hang the probe could not explain.
    const HANG: Duration = Duration::from_secs(20);

    /// Detects and counts lost wake-ups instead of hanging on them.
    pub(super) struct Probe {
        waiting: Waiting,
        /// The round the waiting side is in; set before it starts to wait.
        round: AtomicUsize,
        /// The last round whose notifier has finished its push or pop and notify check.
        notified: AtomicUsize,
        lost: AtomicUsize,
        /// Set when the probe ended a stuck wait, so an event waiter returns instead of
        /// sleeping again for a notification that never comes.
        recovered: AtomicBool,
    }

    impl Probe {
        pub(super) fn new(waiting: Waiting) -> Self {
            Self {
                waiting,
                round: AtomicUsize::new(0),
                notified: AtomicUsize::new(0),
                lost: AtomicUsize::new(0),
                recovered: AtomicBool::new(false),
            }
        }

        pub(super) fn recovered(&self) -> bool {
            self.recovered.swap(false, StdOrdering::SeqCst)
        }

        /// Whether the waiting side's condition already holds and its notifier is done, so
        /// nothing will wake it.
        fn stuck(&self, header: &RingHeader<u64>, capacity: u32) -> bool {
            if self.notified.load(StdOrdering::SeqCst) != self.round.load(StdOrdering::SeqCst) {
                return false;
            }
            let head = header.head.load(Ordering::SeqCst) & INDEX_MASK;
            let tail = header.tail.load(Ordering::SeqCst) & INDEX_MASK;
            let filled = tail.wrapping_sub(head) & INDEX_MASK;
            match self.waiting {
                Waiting::Consumer => filled != 0,
                Waiting::Producer => filled < capacity,
            }
        }

        pub(super) fn sleep<'a>(
            &self,
            header: &RingHeader<u64>,
            capacity: u32,
            wake: &Condvar,
            mut events: MutexGuard<'a, Events>,
            pending: fn(&mut Events) -> &mut usize,
        ) -> MutexGuard<'a, Events> {
            let started = Instant::now();
            let sent_before = *pending(&mut events);
            loop {
                let (guard, timeout) = wake
                    .wait_timeout(events, SLICE)
                    .expect("no test thread panics while holding the lock");
                events = guard;
                // A notification that raced the timeout still counts as delivered.
                if !timeout.timed_out() || *pending(&mut events) != sent_before {
                    return events;
                }
                if self.stuck(header, capacity) {
                    self.lost.fetch_add(1, StdOrdering::SeqCst);
                    self.recovered.store(true, StdOrdering::SeqCst);
                    return events;
                }
                assert!(started.elapsed() < HANG, "wait hung without a lost wake-up");
            }
        }
    }

    /// The races need both threads on a CPU at once, so the stress tests run one at a time.
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn rounds() -> usize {
        env::var("RINGS_STRESS_ROUNDS")
            .ok()
            .and_then(|rounds| rounds.parse().ok())
            .unwrap_or(50_000)
    }

    /// A small xorshift generator; the delays only need to spread, not to be good random.
    struct Delays(u64);

    impl Delays {
        fn spin(&mut self) {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            for _ in 0..self.0 % 768 {
                core::hint::spin_loop();
            }
        }
    }

    fn wait_for(counter: &AtomicUsize, round: usize) {
        while counter.load(StdOrdering::SeqCst) < round {
            core::hint::spin_loop();
        }
    }

    /// The producer pushes item `n` in round `n` while the consumer starts to wait for it.
    /// Returns the number of lost wake-ups.
    fn push_vs_waiting_consumer(kind: Kind) -> usize {
        let _serial = serial();
        let fixture = Fixture::new(1, kind, Waiting::Consumer);
        let rounds = rounds();
        let probe = &fixture.probe;
        thread::scope(|scope| {
            scope.spawn(|| {
                let mut producer = fixture.producer();
                let mut delays = Delays(0x9e37_79b9_7f4a_7c15);
                for round in 1..=rounds {
                    wait_for(&probe.round, round);
                    delays.spin();
                    push_notify(&fixture, &mut producer, round as u64);
                    probe.notified.store(round, StdOrdering::SeqCst);
                }
            });
            let mut consumer = fixture.consumer();
            for round in 1..=rounds {
                // Start each round with the last notification delivered and no stale event,
                // so a lost wake-up is not hidden by an earlier one.
                wait_for(&probe.notified, round - 1);
                *fixture.lock() = Events::default();
                probe.round.store(round, StdOrdering::SeqCst);
                assert_eq!(pop_waiting(&fixture, &mut consumer), round as u64);
            }
        });
        fixture.probe.lost.load(StdOrdering::SeqCst)
    }

    /// The ring is full; the consumer pops one item while the producer starts to wait for room.
    fn pop_vs_waiting_producer(kind: Kind) -> usize {
        let _serial = serial();
        let fixture = Fixture::new(1, kind, Waiting::Producer);
        let rounds = rounds();
        let probe = &fixture.probe;
        thread::scope(|scope| {
            scope.spawn(|| {
                let mut consumer = fixture.consumer();
                let mut delays = Delays(0x2545_f491_4f6c_dd1d);
                for round in 1..=rounds {
                    wait_for(&probe.round, round);
                    delays.spin();
                    assert_eq!(pop_notify(&fixture, &mut consumer), round as u64 - 1);
                    probe.notified.store(round, StdOrdering::SeqCst);
                }
            });
            let mut producer = fixture.producer();
            push_notify(&fixture, &mut producer, 0);
            for round in 1..=rounds {
                wait_for(&probe.notified, round - 1);
                *fixture.lock() = Events::default();
                probe.round.store(round, StdOrdering::SeqCst);
                push_waiting(&fixture, &mut producer, round as u64);
            }
        });
        fixture.probe.lost.load(StdOrdering::SeqCst)
    }

    #[test]
    fn push_vs_pop_sync_with_futex_waiter() {
        assert_eq!(push_vs_waiting_consumer(Kind::Futex), 0, "lost wake-ups");
    }

    #[test]
    fn push_vs_pop_async_with_event_waiter() {
        assert_eq!(push_vs_waiting_consumer(Kind::Event), 0, "lost wake-ups");
    }

    #[test]
    fn pop_vs_push_sync_with_futex_waiter() {
        assert_eq!(pop_vs_waiting_producer(Kind::Futex), 0, "lost wake-ups");
    }

    #[test]
    fn pop_vs_push_async_with_event_waiter() {
        assert_eq!(pop_vs_waiting_producer(Kind::Event), 0, "lost wake-ups");
    }
}

#[cfg(loom)]
mod model {
    //! Exhaustive models of one notification racing one wait. A lost wake-up leaves the waiter
    //! blocked after the notifier has finished, which loom reports as a deadlock.

    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Run `test` under every interleaving, and check loom really explored some: a handshake
    /// with no branch points would pass without testing anything.
    fn model(test: impl Fn() + Sync + Send + 'static) {
        use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};
        static EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

        let mut builder = loom::model::Builder::new();
        let before = EXECUTIONS.load(StdOrdering::SeqCst);
        builder.check(move || {
            EXECUTIONS.fetch_add(1, StdOrdering::SeqCst);
            test();
        });
        let explored = EXECUTIONS.load(StdOrdering::SeqCst) - before;
        std::eprintln!("loom explored {explored} executions");
        assert!(explored > 10, "loom explored only {explored} executions");
    }

    fn push_vs_waiting_consumer(kind: Kind) {
        model(move || {
            let fixture = Arc::new(Fixture::new(2, kind, Waiting::Consumer));
            let notifier = Arc::clone(&fixture);
            let producer = thread::spawn(move || {
                push_notify(&notifier, &mut notifier.producer(), 1);
            });
            assert_eq!(pop_waiting(&fixture, &mut fixture.consumer()), 1);
            producer.join().expect("the producer does not panic");
        });
    }

    fn pop_vs_waiting_producer(kind: Kind) {
        model(move || {
            let fixture = Arc::new(Fixture::new(1, kind, Waiting::Producer));
            let mut producer = fixture.producer();
            push_notify(&fixture, &mut producer, 1);
            let notifier = Arc::clone(&fixture);
            let consumer = thread::spawn(move || {
                assert_eq!(pop_notify(&notifier, &mut notifier.consumer()), 1);
            });
            push_waiting(&fixture, &mut producer, 2);
            consumer.join().expect("the consumer does not panic");
        });
    }

    #[test]
    fn push_vs_pop_sync_with_futex_waiter() {
        push_vs_waiting_consumer(Kind::Futex);
    }

    #[test]
    fn push_vs_pop_async_with_event_waiter() {
        push_vs_waiting_consumer(Kind::Event);
    }

    #[test]
    fn pop_vs_push_sync_with_futex_waiter() {
        pop_vs_waiting_producer(Kind::Futex);
    }

    #[test]
    fn pop_vs_push_async_with_event_waiter() {
        pop_vs_waiting_producer(Kind::Event);
    }
}
