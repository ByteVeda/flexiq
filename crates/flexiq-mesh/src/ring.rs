use std::collections::BTreeMap;

use xxhash_rust::xxh3::xxh3_64;

/// Consistent hash ring with virtual nodes for even distribution.
///
/// Maps task names to preferred worker IDs. Workers are placed on the ring
/// via `virtual_nodes` points each; tasks hash to the next clockwise worker.
pub struct HashRing {
    ring: BTreeMap<u64, String>,
    virtual_nodes: usize,
}

impl HashRing {
    pub fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
        }
    }

    /// Add a worker to the ring with `virtual_nodes` points.
    pub fn add_worker(&mut self, worker_id: &str) {
        for i in 0..self.virtual_nodes {
            let key = format!("{worker_id}-vnode-{i}");
            let hash = xxh3_64(key.as_bytes());
            self.ring.insert(hash, worker_id.to_string());
        }
    }

    /// Remove a worker and all its virtual nodes from the ring.
    pub fn remove_worker(&mut self, worker_id: &str) {
        self.ring.retain(|_, v| v != worker_id);
    }

    /// Look up the preferred worker for a given key (e.g., task_name).
    /// Returns `None` if the ring is empty.
    pub fn preferred_worker(&self, key: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = xxh3_64(key.as_bytes());
        // Find the first node clockwise from the hash point
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, worker_id)| worker_id.as_str())
    }

    /// Check whether a specific worker is the preferred owner for a key.
    pub fn is_owner(&self, key: &str, worker_id: &str) -> bool {
        self.preferred_worker(key) == Some(worker_id)
    }

    /// Number of workers (not virtual nodes) on the ring.
    pub fn worker_count(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        for worker_id in self.ring.values() {
            seen.insert(worker_id.as_str());
        }
        seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_placement() {
        let mut ring = HashRing::new(150);
        ring.add_worker("worker-a");
        ring.add_worker("worker-b");
        ring.add_worker("worker-c");

        let owner1 = ring.preferred_worker("my_task").unwrap();
        let owner2 = ring.preferred_worker("my_task").unwrap();
        assert_eq!(owner1, owner2, "same key must always map to same worker");
    }

    #[test]
    fn different_tasks_may_map_differently() {
        let mut ring = HashRing::new(150);
        ring.add_worker("worker-a");
        ring.add_worker("worker-b");

        // With 2 workers and 150 vnodes each, different task names should
        // spread across workers (probabilistic but near-certain for these names).
        let mut owners = std::collections::HashSet::new();
        for i in 0..20 {
            owners.insert(
                ring.preferred_worker(&format!("task_{i}"))
                    .unwrap()
                    .to_string(),
            );
        }
        assert!(owners.len() > 1, "tasks should distribute across workers");
    }

    #[test]
    fn minimal_key_migration_on_remove() {
        let mut ring = HashRing::new(150);
        ring.add_worker("worker-a");
        ring.add_worker("worker-b");
        ring.add_worker("worker-c");

        let tasks: Vec<String> = (0..100).map(|i| format!("task_{i}")).collect();
        let before: Vec<String> = tasks
            .iter()
            .map(|t| ring.preferred_worker(t).unwrap().to_string())
            .collect();

        ring.remove_worker("worker-b");

        let mut migrated = 0;
        for (i, task) in tasks.iter().enumerate() {
            let after = ring.preferred_worker(task).unwrap();
            if after != before[i] {
                migrated += 1;
            }
        }

        // With consistent hashing, removing 1 of 3 workers should migrate
        // roughly 1/3 of keys, not all of them.
        assert!(
            migrated < 60,
            "expected < 60% migration, got {migrated}/100"
        );
    }

    #[test]
    fn even_distribution() {
        let mut ring = HashRing::new(150);
        ring.add_worker("w1");
        ring.add_worker("w2");
        ring.add_worker("w3");

        let mut counts = std::collections::HashMap::new();
        for i in 0..3000 {
            let owner = ring.preferred_worker(&format!("key_{i}")).unwrap();
            *counts.entry(owner.to_string()).or_insert(0) += 1;
        }

        for (worker, count) in &counts {
            // Each worker should get roughly 1000 ± 300 (30% tolerance)
            assert!(
                *count > 700 && *count < 1300,
                "worker {worker} got {count}/3000 keys — distribution too uneven"
            );
        }
    }

    #[test]
    fn is_owner_check() {
        let mut ring = HashRing::new(150);
        ring.add_worker("worker-a");
        ring.add_worker("worker-b");

        let owner = ring.preferred_worker("test_task").unwrap().to_string();
        assert!(ring.is_owner("test_task", &owner));
    }

    #[test]
    fn empty_ring() {
        let ring = HashRing::new(150);
        assert!(ring.preferred_worker("anything").is_none());
        assert!(ring.is_empty());
        assert_eq!(ring.worker_count(), 0);
    }

    #[test]
    fn add_remove_worker() {
        let mut ring = HashRing::new(150);
        ring.add_worker("worker-a");
        assert_eq!(ring.worker_count(), 1);

        ring.add_worker("worker-b");
        assert_eq!(ring.worker_count(), 2);

        ring.remove_worker("worker-a");
        assert_eq!(ring.worker_count(), 1);

        // All keys now map to worker-b
        assert_eq!(ring.preferred_worker("any_task").unwrap(), "worker-b");
    }
}
