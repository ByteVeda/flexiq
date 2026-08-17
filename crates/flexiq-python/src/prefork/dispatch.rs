//! Job dispatch strategies for distributing work across child processes.

/// Selects the child with the fewest in-flight jobs (least-loaded).
/// Falls back to round-robin if all children have equal load.
pub fn least_loaded(in_flight_counts: &[u32]) -> usize {
    in_flight_counts
        .iter()
        .enumerate()
        .min_by_key(|(_, &count)| count)
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

/// Selects the child with the lowest weighted load.
///
/// Score = in_flight_count * avg_task_duration_ns. A worker with 1 slow job
/// scores higher than one with 3 fast jobs, enabling better load distribution
/// across heterogeneous workloads.
///
/// Falls back to `least_loaded` when `avg_duration_ns` is 0.
#[allow(dead_code)]
pub fn weighted_least_loaded(in_flight_counts: &[u32], avg_duration_ns: i64) -> usize {
    if avg_duration_ns <= 0 {
        return least_loaded(in_flight_counts);
    }
    in_flight_counts
        .iter()
        .enumerate()
        .min_by_key(|(_, &count)| count as i64 * avg_duration_ns)
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_least_loaded_picks_idle() {
        assert_eq!(least_loaded(&[3, 0, 2]), 1);
    }

    #[test]
    fn test_least_loaded_picks_first_on_tie() {
        assert_eq!(least_loaded(&[1, 1, 1]), 0);
    }

    #[test]
    fn test_least_loaded_single() {
        assert_eq!(least_loaded(&[5]), 0);
    }

    #[test]
    fn test_weighted_picks_lowest_score() {
        // Worker 0: 2 in-flight * 100ns = 200
        // Worker 1: 1 in-flight * 100ns = 100 ← pick this
        // Worker 2: 3 in-flight * 100ns = 300
        assert_eq!(weighted_least_loaded(&[2, 1, 3], 100), 1);
    }

    #[test]
    fn test_weighted_falls_back_on_zero_duration() {
        assert_eq!(weighted_least_loaded(&[3, 0, 2], 0), 1); // same as least_loaded
    }
}
