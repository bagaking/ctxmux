//! Run-local live delivery, funded by actual allocations rather than an event
//! count. Original output/replay and the Run's revision cursor remain truth.

use std::{
    collections::VecDeque,
    mem::size_of,
    sync::{Arc, Mutex},
};
use tokio::sync::{
    Notify,
    broadcast::error::{RecvError, TryRecvError},
};

use crate::{
    LiveRunEvent, RunEvent, mutex_lock,
    resources::{ByteBudget, BytePermit},
};

pub(super) struct Sender {
    shared: Arc<Shared>,
}

pub(super) struct Receiver {
    shared: Arc<Shared>,
    reader: usize,
}

struct Shared {
    state: Mutex<State>,
    ready: Notify,
    budget: ByteBudget,
    _memory: BytePermit,
}

struct State {
    // One inline envelope makes a pressure notification possible without a
    // second allocation. It is not a retention ceiling: a funded deque grows
    // for all further events needed by actual receivers.
    first: Option<LiveRunEvent>,
    rest: VecDeque<LiveRunEvent>,
    rest_memory: Option<ArrayMemory>,
    base: u64,
    next: u64,
    readers: Vec<Option<u64>>,
    readers_memory: Option<ArrayMemory>,
    receiver_count: usize,
    closed: bool,
}

struct ArrayMemory {
    _requested: BytePermit,
    _extra: Option<BytePermit>,
}

pub(super) struct Prepared {
    pub memory: Option<BytePermit>,
    pub retired_output: bool,
}

impl Sender {
    pub fn new(budget: ByteBudget) -> Option<Self> {
        let memory = budget.reserve(size_of::<Shared>().checked_add(2 * size_of::<usize>())?)?;
        Some(Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    first: None,
                    rest: VecDeque::new(),
                    rest_memory: None,
                    base: 0,
                    next: 0,
                    readers: Vec::new(),
                    readers_memory: None,
                    receiver_count: 0,
                    closed: false,
                }),
                ready: Notify::new(),
                budget,
                _memory: memory,
            }),
        })
    }

    pub fn subscribe(&self) -> Option<Receiver> {
        let mut state = mutex_lock(&self.shared.state);
        if state.closed {
            return None;
        }
        let reader = if let Some(index) = state.readers.iter().position(Option::is_none) {
            index
        } else {
            if state.readers.len() == state.readers.capacity() {
                let capacity = state.readers.capacity().checked_mul(2)?.max(1);
                let (mut readers, memory) = funded_vec(&self.shared.budget, capacity)?;
                readers.append(&mut state.readers);
                state.readers = readers;
                state.readers_memory = Some(memory);
            }
            state.readers.push(None);
            state.readers.len() - 1
        };
        state.readers[reader] = Some(state.next);
        state.receiver_count += 1;
        Some(Receiver {
            shared: Arc::clone(&self.shared),
            reader,
        })
    }

    pub fn receiver_count(&self) -> usize {
        mutex_lock(&self.shared.state).receiver_count
    }

    // Called by the serialized Run publication owner before it creates the
    // event cursor. Retirement facts therefore reach the next envelope's
    // BEFORE cursor, including when the retired event was itself a Gap.
    pub fn prepare(&self, allocation: Option<usize>) -> Prepared {
        let mut state = mutex_lock(&self.shared.state);
        state.reclaim();
        let mut retired_output = false;
        if state.first.is_some() && state.rest.len() == state.rest.capacity() {
            let capacity = state
                .rest
                .capacity()
                .checked_mul(2)
                .map(|capacity| capacity.max(1));
            let grown = capacity.and_then(|capacity| funded_deque(&self.shared.budget, capacity));
            if let Some((mut rest, memory)) = grown {
                rest.append(&mut state.rest);
                state.rest = rest;
                state.rest_memory = Some(memory);
            } else {
                retired_output |= state.retire();
            }
        }
        let memory = allocation.and_then(|bytes| {
            loop {
                if let Some(memory) = self.shared.budget.reserve(bytes) {
                    break Some(memory);
                }
                if state.first.is_none() {
                    break None;
                }
                retired_output |= state.retire();
            }
        });
        Prepared {
            memory,
            retired_output,
        }
    }

    pub fn send(&self, event: LiveRunEvent) {
        let mut state = mutex_lock(&self.shared.state);
        if state.closed || state.receiver_count == 0 {
            return;
        }
        state.next = state
            .next
            .checked_add(1)
            .expect("event sequence remains representable");
        if state.first.is_none() {
            state.first = Some(event);
        } else {
            // prepare funded or retired the required slot. Subscribers only
            // consume; no receiver can increase this deque between calls.
            assert!(state.rest.len() < state.rest.capacity());
            state.rest.push_back(event);
        }
        drop(state);
        self.shared.ready.notify_waiters();
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        mutex_lock(&self.shared.state).closed = true;
        self.shared.ready.notify_waiters();
    }
}

impl State {
    fn retire(&mut self) -> bool {
        let Some(retired) = self.first.take() else {
            return false;
        };
        let output = retired.event().is_some_and(|event| {
            matches!(
                event.as_ref(),
                RunEvent::Output { .. } | RunEvent::Gap { .. }
            )
        });
        self.first = self.rest.pop_front();
        self.base += 1;
        if self.first.is_none() {
            // Real backlog has disappeared: release its container, too. The
            // inline envelope continues without heap-slot preallocation.
            self.rest = VecDeque::new();
            self.rest_memory = None;
        }
        output
    }

    fn reclaim(&mut self) {
        let through = self
            .readers
            .iter()
            .flatten()
            .copied()
            .min()
            .unwrap_or(self.next);
        while self.base < through {
            self.retire();
        }
    }
}

impl Receiver {
    pub fn try_recv(&mut self) -> Result<LiveRunEvent, TryRecvError> {
        let mut state = mutex_lock(&self.shared.state);
        let position = state.readers[self.reader].expect("registered live receiver");
        if position < state.base {
            state.readers[self.reader] = Some(state.base);
            return Err(TryRecvError::Lagged(state.base - position));
        }
        if position == state.next {
            return Err(if state.closed {
                TryRecvError::Closed
            } else {
                TryRecvError::Empty
            });
        }
        let offset =
            usize::try_from(position - state.base).expect("funded journal index fits host");
        let event = if offset == 0 {
            state.first.as_ref().expect("retained first event").clone()
        } else {
            state.rest[offset - 1].clone()
        };
        state.readers[self.reader] = Some(position + 1);
        state.reclaim();
        Ok(event)
    }

    pub async fn recv(&mut self) -> Result<LiveRunEvent, RecvError> {
        loop {
            let shared = Arc::clone(&self.shared);
            let ready = shared.ready.notified();
            tokio::pin!(ready);
            // Register before checking the queue: notify_waiters does not
            // retain a permit for a not-yet-registered future.
            ready.as_mut().enable();
            match self.try_recv() {
                Ok(event) => return Ok(event),
                Err(TryRecvError::Closed) => return Err(RecvError::Closed),
                Err(TryRecvError::Lagged(count)) => return Err(RecvError::Lagged(count)),
                Err(TryRecvError::Empty) => ready.await,
            }
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let mut state = mutex_lock(&self.shared.state);
        state.readers[self.reader] = None;
        state.receiver_count -= 1;
        state.reclaim();
        if state.receiver_count == 0 {
            state.readers = Vec::new();
            state.readers_memory = None;
        }
        drop(state);
        self.shared.ready.notify_waiters();
    }
}

// Reserve the replacement while its predecessor is still owned. Allocation
// failure leaves the old journal intact. If a collection reports more slots
// than requested, fund those slots before installing the collection.
fn funded_vec<T>(budget: &ByteBudget, capacity: usize) -> Option<(Vec<T>, ArrayMemory)> {
    let requested = budget.reserve(capacity.checked_mul(size_of::<T>())?)?;
    let mut vector = Vec::new();
    vector.try_reserve_exact(capacity).ok()?;
    let extra = fund_extra::<T>(budget, capacity, vector.capacity()).ok()?;
    Some((
        vector,
        ArrayMemory {
            _requested: requested,
            _extra: extra,
        },
    ))
}

fn funded_deque(
    budget: &ByteBudget,
    capacity: usize,
) -> Option<(VecDeque<LiveRunEvent>, ArrayMemory)> {
    let requested = budget.reserve(capacity.checked_mul(size_of::<LiveRunEvent>())?)?;
    let mut deque = VecDeque::new();
    deque.try_reserve_exact(capacity).ok()?;
    let extra = fund_extra::<LiveRunEvent>(budget, capacity, deque.capacity()).ok()?;
    Some((
        deque,
        ArrayMemory {
            _requested: requested,
            _extra: extra,
        },
    ))
}

fn fund_extra<T>(
    budget: &ByteBudget,
    requested: usize,
    actual: usize,
) -> Result<Option<BytePermit>, ()> {
    let extra = actual
        .checked_sub(requested)
        .and_then(|slots| slots.checked_mul(size_of::<T>()))
        .ok_or(())?;
    if extra == 0 {
        Ok(None)
    } else {
        budget.reserve(extra).map(Some).ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LiveEventCursor, PublishedRunEvent};

    fn marker(byte: u64) -> LiveRunEvent {
        LiveRunEvent {
            published: PublishedRunEvent::OutputGap(
                byte,
                ctxmux_protocol::OutputGapCauses::UNKNOWN,
            ),
            before: LiveEventCursor::default(),
            after: LiveEventCursor {
                output_bytes: byte,
                ..LiveEventCursor::default()
            },
        }
    }

    #[tokio::test]
    async fn preserves_a_backlog_longer_than_the_historical_256_slots() {
        let budget = ByteBudget::new(4 * 1024 * 1024);
        let sender = Sender::new(budget.clone()).unwrap();
        let mut first = sender.subscribe().unwrap();
        let mut second = sender.subscribe().unwrap();
        let idle = budget.used();
        for byte in 0..1025 {
            assert!(!sender.prepare(None).retired_output);
            sender.send(marker(byte));
            assert_eq!(first.recv().await.unwrap().after.output_bytes, byte);
        }
        assert!(budget.used() > idle);
        for byte in 0..1025 {
            assert_eq!(second.recv().await.unwrap().after.output_bytes, byte);
        }
        assert_eq!(
            budget.used(),
            idle,
            "consumed backlog releases its actual allocation"
        );
        drop(sender);
        drop(first);
        drop(second);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn exact_byte_pressure_retires_history_and_needs_no_marker_allocation() {
        let budget = ByteBudget::new(1024 * 1024);
        let sender = Sender::new(budget.clone()).unwrap();
        let mut receiver = sender.subscribe().unwrap();
        sender.prepare(None);
        sender.send(marker(1));
        let held = budget
            .reserve(1024 * 1024 - usize::try_from(budget.used()).unwrap())
            .unwrap();
        assert!(sender.prepare(None).retired_output);
        sender.send(marker(2));
        assert!(matches!(receiver.recv().await, Err(RecvError::Lagged(1))));
        assert_eq!(receiver.recv().await.unwrap().after.output_bytes, 2);
        drop(held);
        drop(sender);
        drop(receiver);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn receiver_drop_releases_only_its_unneeded_backlog_and_reuses_registry_slot() {
        let budget = ByteBudget::new(1024 * 1024);
        let sender = Sender::new(budget.clone()).unwrap();
        let mut first = sender.subscribe().unwrap();
        let second = sender.subscribe().unwrap();
        let idle = budget.used();
        for byte in 0..256 {
            sender.prepare(None);
            sender.send(marker(byte));
            assert_eq!(first.recv().await.unwrap().after.output_bytes, byte);
        }
        drop(second);
        assert_eq!(budget.used(), idle);
        let mut replacement = sender.subscribe().unwrap();
        sender.prepare(None);
        sender.send(marker(256));
        assert_eq!(first.recv().await.unwrap().after.output_bytes, 256);
        assert_eq!(replacement.recv().await.unwrap().after.output_bytes, 256);
    }

    #[tokio::test]
    async fn closes_waiting_receivers_and_drains_a_published_boundary_first() {
        let sender = Sender::new(ByteBudget::new(1024 * 1024)).unwrap();
        let mut receiver = sender.subscribe().unwrap();
        sender.prepare(None);
        sender.send(marker(1));
        drop(sender);
        assert_eq!(receiver.recv().await.unwrap().after.output_bytes, 1);
        assert!(matches!(receiver.recv().await, Err(RecvError::Closed)));
        let sender = Sender::new(ByteBudget::new(1024 * 1024)).unwrap();
        let mut receiver = sender.subscribe().unwrap();
        let waiter = tokio::spawn(async move { receiver.recv().await });
        tokio::task::yield_now().await;
        drop(sender);
        assert!(matches!(waiter.await.unwrap(), Err(RecvError::Closed)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_waiting_receivers_keep_every_boundary_across_empty_queue_races() {
        // Exercise publication before and after each receiver registers its
        // wait. The sample count exceeds the former event ring; it is a test
        // schedule, not a product retention or receiver-population limit.
        let sender = Sender::new(ByteBudget::new(1024 * 1024)).unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut readers = tokio::task::JoinSet::new();
        for _ in 0..2 {
            let mut receiver = sender.subscribe().unwrap();
            let barrier = Arc::clone(&barrier);
            readers.spawn(async move {
                for byte in 0..1025 {
                    barrier.wait().await;
                    assert_eq!(receiver.recv().await.unwrap().after.output_bytes, byte);
                }
                assert!(matches!(receiver.recv().await, Err(RecvError::Closed)));
            });
        }
        for byte in 0..1025 {
            barrier.wait().await;
            tokio::task::yield_now().await;
            assert!(!sender.prepare(None).retired_output);
            sender.send(marker(byte));
        }
        drop(sender);
        while let Some(result) = readers.join_next().await {
            result.unwrap();
        }
    }

    #[test]
    fn default_budget_funds_8192_idle_journals_without_a_fixed_ring() {
        let budget = ByteBudget::new(crate::ResourceLimits::DEFAULT.live_event_bytes);
        let mut journals = Vec::new();
        for _ in 0..8192 {
            let sender = Sender::new(budget.clone()).unwrap();
            let receiver = sender.subscribe().unwrap();
            journals.push((sender, receiver));
        }
        assert!(budget.used() < crate::ResourceLimits::DEFAULT.live_event_bytes);
        drop(journals);
        assert_eq!(budget.used(), 0);
    }
}
