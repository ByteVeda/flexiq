//! How many watches each credential holds, against a cap.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// Open watches per credential id.
#[derive(Debug)]
pub struct Quota {
    /// Zero is unbounded.
    cap: usize,
    held: Arc<Mutex<HashMap<Arc<str>, usize>>>,
}

/// One held watch; releases its slot when dropped, however the stream ended.
#[derive(Debug)]
pub struct Slot {
    credential: Arc<str>,
    held: Arc<Mutex<HashMap<Arc<str>, usize>>>,
}

impl Quota {
    /// `cap` watches per credential; zero is unbounded.
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            held: Arc::default(),
        }
    }

    /// The cap, for the refusal to name.
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Take a slot for `credential`, or `None` when it already holds the cap.
    pub fn acquire(&self, credential: &Arc<str>) -> Option<Slot> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let count = held.entry(Arc::clone(credential)).or_default();
        if self.cap != 0 && *count >= self.cap {
            return None;
        }
        *count += 1;
        Some(Slot {
            credential: Arc::clone(credential),
            held: Arc::clone(&self.held),
        })
    }

    /// Watches open across every credential.
    pub fn total(&self) -> usize {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .sum()
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = held.get_mut(&self.credential) {
            *count -= 1;
            // Dropped at zero, so a credential that stops watching costs nothing.
            if *count == 0 {
                held.remove(&self.credential);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_credential_gets_its_cap_and_no_more_and_gets_it_back() {
        let quota = Quota::new(2);
        let alice: Arc<str> = "alice".into();
        let bob: Arc<str> = "bob".into();
        let first = quota.acquire(&alice).expect("under the cap");
        let _second = quota.acquire(&alice).expect("at the cap");
        assert!(quota.acquire(&alice).is_none());
        assert!(quota.acquire(&bob).is_some(), "the cap is per credential");
        drop(first);
        assert!(
            quota.acquire(&alice).is_some(),
            "a dropped slot is released"
        );
    }

    #[test]
    fn zero_is_unbounded() {
        let quota = Quota::new(0);
        let alice: Arc<str> = "alice".into();
        let slots: Vec<_> = (0..100).filter_map(|_| quota.acquire(&alice)).collect();
        assert_eq!(slots.len(), 100);
        assert_eq!(quota.total(), 100);
        drop(slots);
        assert_eq!(quota.total(), 0);
    }
}
