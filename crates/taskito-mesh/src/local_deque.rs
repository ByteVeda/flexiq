use std::collections::VecDeque;
use std::sync::Mutex;

use taskito_core::job::Job;

/// Thread-safe local job buffer.
///
/// Owner pushes to the back and pops from the back (LIFO for affinity-hot
/// jobs). Stealers pop from the front (cold end). Protected by a mutex;
/// contention is negligible since steals are infrequent.
pub struct LocalDeque {
    inner: Mutex<VecDeque<Job>>,
    capacity: usize,
}

// Safety: Mutex<VecDeque<Job>> is Send+Sync when Job is Send.
unsafe impl Send for LocalDeque {}
unsafe impl Sync for LocalDeque {}

impl LocalDeque {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Push a job to the back (owner end). Returns false if at capacity.
    pub fn push(&self, job: Job) -> bool {
        let mut deque = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if deque.len() >= self.capacity {
            return false;
        }
        deque.push_back(job);
        true
    }

    /// Pop a job from the back (owner end, hot/affinity side — LIFO).
    pub fn pop(&self) -> Option<Job> {
        let mut deque = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        deque.pop_back()
    }

    /// Steal up to `count` jobs from the front (cold/stealable end — FIFO).
    pub fn steal(&self, count: usize) -> Vec<Job> {
        let mut deque = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let n = count.min(deque.len());
        let mut stolen = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(job) = deque.pop_front() {
                stolen.push(job);
            }
        }
        stolen
    }

    pub fn len(&self) -> usize {
        let deque = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        deque.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Push multiple jobs, sorting by affinity: owned tasks go to the back
    /// (popped first via LIFO), non-owned go to the front (stealable end).
    pub fn push_sorted<F>(&self, mut jobs: Vec<Job>, is_affinity_owner: F) -> usize
    where
        F: Fn(&str) -> bool,
    {
        let mut deque = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let remaining = self.capacity.saturating_sub(deque.len());
        jobs.truncate(remaining);

        // Partition: non-affinity first (front/stealable), affinity last (back/hot)
        jobs.sort_by_key(|j| {
            if is_affinity_owner(&j.task_name) {
                1
            } else {
                0
            }
        });

        let pushed = jobs.len();
        for job in jobs {
            deque.push_back(job);
        }
        pushed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taskito_core::job::{now_millis, NewJob};

    fn make_job(task_name: &str) -> Job {
        NewJob {
            queue: "default".to_string(),
            task_name: task_name.to_string(),
            payload: vec![],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 0,
            timeout_ms: 30_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            debounce_key: None,
        }
        .into_job()
    }

    #[test]
    fn push_pop_lifo_order() {
        let deque = LocalDeque::new(10);
        deque.push(make_job("first"));
        deque.push(make_job("second"));
        deque.push(make_job("third"));

        assert_eq!(deque.len(), 3);
        assert_eq!(deque.pop().unwrap().task_name, "third");
        assert_eq!(deque.pop().unwrap().task_name, "second");
        assert_eq!(deque.pop().unwrap().task_name, "first");
        assert!(deque.pop().is_none());
    }

    #[test]
    fn capacity_enforced() {
        let deque = LocalDeque::new(2);
        assert!(deque.push(make_job("a")));
        assert!(deque.push(make_job("b")));
        assert!(!deque.push(make_job("c")));
        assert_eq!(deque.len(), 2);
    }

    #[test]
    fn steal_takes_from_front() {
        let deque = LocalDeque::new(10);
        deque.push(make_job("first"));
        deque.push(make_job("second"));
        deque.push(make_job("third"));

        let stolen = deque.steal(2);
        assert_eq!(stolen.len(), 2);
        assert_eq!(stolen[0].task_name, "first");
        assert_eq!(stolen[1].task_name, "second");

        // Owner pops the remaining one from back
        assert_eq!(deque.pop().unwrap().task_name, "third");
    }

    #[test]
    fn push_sorted_affinity_at_back() {
        let deque = LocalDeque::new(10);
        let jobs = vec![
            make_job("cold_task"),
            make_job("hot_task"),
            make_job("another_cold"),
        ];

        let pushed = deque.push_sorted(jobs, |name| name == "hot_task");
        assert_eq!(pushed, 3);

        // Popping from back (LIFO): affinity jobs were pushed last → popped first
        assert_eq!(deque.pop().unwrap().task_name, "hot_task");
    }

    #[test]
    fn push_sorted_respects_capacity() {
        let deque = LocalDeque::new(2);
        let jobs = vec![make_job("a"), make_job("b"), make_job("c")];
        let pushed = deque.push_sorted(jobs, |_| false);
        assert_eq!(pushed, 2);
        assert_eq!(deque.len(), 2);
    }

    #[test]
    fn empty_deque() {
        let deque = LocalDeque::new(10);
        assert!(deque.is_empty());
        assert_eq!(deque.len(), 0);
        assert!(deque.pop().is_none());

        let stolen = deque.steal(5);
        assert!(stolen.is_empty());
    }
}
