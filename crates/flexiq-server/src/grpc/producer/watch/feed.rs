//! [`WatchFeed`]: the recent job transitions this process announced, in order,
//! for every `WatchJobs` stream to read at its own pace.
//!
//! It is an [`EventTap`] on the process's event hub, so it sees exactly what
//! the scheduler and the doors already emit — no second source. Each event gets
//! a sequence number; the last `capacity` are kept. A stream remembers the last
//! number it read, and one whose number has been evicted has fallen behind.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use flexiq_core::{EventTap, JobEvent};
use tokio::sync::watch;

/// A position in the feed: every event with a larger number is still to come.
pub type Seq = u64;

/// Reading from a position the feed has already evicted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Evicted;

/// The bounded, ordered record of recent transitions.
pub struct WatchFeed {
    /// Identifies this process's numbering, so a cursor from another process
    /// or an earlier run is recognised rather than misread.
    instance: u64,
    capacity: usize,
    ring: Mutex<Ring>,
    /// The newest sequence number, so a stream can wait for a change without
    /// polling and without missing one that lands between its read and its wait.
    head: watch::Sender<Seq>,
}

#[derive(Default)]
struct Ring {
    /// The number the next event gets. Starts at 1, so position 0 means "from
    /// the beginning".
    next: Seq,
    events: VecDeque<Arc<JobEvent>>,
}

impl Ring {
    /// The number of the oldest event still held.
    fn oldest(&self) -> Seq {
        self.next - self.events.len() as Seq
    }
}

impl WatchFeed {
    /// A feed holding up to `capacity` events, at least one.
    pub fn new(capacity: usize) -> Self {
        Self {
            instance: rand::random(),
            capacity: capacity.max(1),
            ring: Mutex::new(Ring {
                next: 1,
                events: VecDeque::new(),
            }),
            head: watch::Sender::new(0),
        }
    }

    /// This process's numbering.
    pub fn instance(&self) -> u64 {
        self.instance
    }

    /// The newest position: everything up to it has been emitted.
    pub fn head(&self) -> Seq {
        *self.head.borrow()
    }

    /// A receiver that wakes whenever the head moves.
    pub fn subscribe(&self) -> watch::Receiver<Seq> {
        self.head.subscribe()
    }

    /// Every event after `after`, oldest first, each with its number.
    ///
    /// [`Evicted`] when an event after `after` has already been dropped: the
    /// reader would otherwise skip it without knowing.
    pub fn read_after(&self, after: Seq) -> Result<Vec<(Seq, Arc<JobEvent>)>, Evicted> {
        let ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        let oldest = ring.oldest();
        if after + 1 < oldest {
            return Err(Evicted);
        }
        let skip = usize::try_from(after + 1 - oldest).unwrap_or(usize::MAX);
        Ok(ring
            .events
            .iter()
            .skip(skip)
            .enumerate()
            .map(|(i, event)| (after + 1 + i as Seq, Arc::clone(event)))
            .collect())
    }

    /// Whether `after` is a position a reader could resume from: not evicted,
    /// and not in the future.
    pub fn holds(&self, after: Seq) -> bool {
        let ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        after + 1 >= ring.oldest() && after < ring.next
    }

    fn push(&self, event: Arc<JobEvent>) {
        let seq = {
            let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
            if ring.events.len() == self.capacity {
                ring.events.pop_front();
            }
            ring.events.push_back(event);
            let seq = ring.next;
            ring.next += 1;
            seq
        };
        // Outside the lock: a reader woken by this reads the ring. Numbers are
        // taken under the lock, so a racing push can only move it further.
        self.head.send_modify(|head| *head = (*head).max(seq));
    }
}

impl EventTap for WatchFeed {
    fn observe(&self, event: &JobEvent) {
        // Streams never carry payloads, so the feed never holds one.
        let mut event = event.clone();
        event.payload = None;
        self.push(Arc::new(event));
    }
}

#[cfg(test)]
mod tests {
    use flexiq_core::EventType;

    use super::*;

    fn event(id: &str) -> JobEvent {
        JobEvent::new(EventType::JobStarted, id, None, "q", "t")
    }

    fn ids(read: &[(Seq, Arc<JobEvent>)]) -> Vec<(Seq, String)> {
        read.iter().map(|(s, e)| (*s, e.job_id.clone())).collect()
    }

    #[test]
    fn a_reader_gets_everything_after_its_position_in_order() {
        let feed = WatchFeed::new(8);
        assert_eq!(feed.head(), 0);
        for id in ["a", "b", "c"] {
            feed.observe(&event(id));
        }
        assert_eq!(feed.head(), 3);
        assert_eq!(
            ids(&feed.read_after(0).unwrap()),
            [(1, "a".into()), (2, "b".into()), (3, "c".into())]
        );
        assert_eq!(ids(&feed.read_after(2).unwrap()), [(3, "c".into())]);
        assert!(feed.read_after(3).unwrap().is_empty());
    }

    #[test]
    fn a_reader_behind_the_window_is_told_so() {
        let feed = WatchFeed::new(2);
        for id in ["a", "b", "c"] {
            feed.observe(&event(id));
        }
        assert_eq!(feed.read_after(0), Err(Evicted));
        assert_eq!(
            ids(&feed.read_after(1).unwrap()),
            [(2, "b".into()), (3, "c".into())]
        );
        assert!(!feed.holds(0));
        assert!(feed.holds(1) && feed.holds(3));
        assert!(!feed.holds(4), "a position from the future is not held");
    }

    #[test]
    fn the_feed_never_keeps_a_payload() {
        let feed = WatchFeed::new(1);
        let mut with_payload = event("a");
        with_payload.payload = Some(vec![1, 2]);
        feed.observe(&with_payload);
        assert!(feed.read_after(0).unwrap()[0].1.payload.is_none());
    }

    #[tokio::test]
    async fn a_subscriber_wakes_on_a_new_event() {
        let feed = WatchFeed::new(4);
        let mut head = feed.subscribe();
        feed.observe(&event("a"));
        head.changed().await.unwrap();
        assert_eq!(*head.borrow(), 1);
    }
}
