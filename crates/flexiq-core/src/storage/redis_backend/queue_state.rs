//! Queue pause state: one Redis set of paused queue names per namespace.
//!
//! The default namespace keeps the key every release before #836 wrote,
//! `queues:paused`, so an upgrade leaves a default-namespace pause in force.
//! Namespace `N` is `queues:paused:<segment>` with the length-prefixed
//! [`namespace_segment`](RedisStorage::namespace_segment). Before #836 that one
//! set held every tenant's pauses; after it, whatever it holds is the default
//! namespace's — the same reading `m0020` gives the Diesel backends' rows.

use redis::Commands;

use super::{map_err, RedisStorage};
use crate::error::Result;

impl RedisStorage {
    /// The paused-queue set of one namespace (`None` = the default).
    fn paused_key(&self, namespace: Option<&str>) -> String {
        match namespace {
            None => self.key(&["queues", "paused"]),
            Some(_) => self.key(&["queues", "paused", &Self::namespace_segment(namespace)]),
        }
    }

    /// Pause a queue so no new jobs are dispatched from it.
    pub fn pause_queue(&self, queue_name: &str, namespace: Option<&str>) -> Result<()> {
        let mut conn = self.conn()?;
        conn.sadd::<_, _, ()>(self.paused_key(namespace), queue_name)
            .map_err(map_err)?;
        Ok(())
    }

    /// Resume a paused queue.
    pub fn resume_queue(&self, queue_name: &str, namespace: Option<&str>) -> Result<()> {
        let mut conn = self.conn()?;
        conn.srem::<_, _, ()>(self.paused_key(namespace), queue_name)
            .map_err(map_err)?;
        Ok(())
    }

    /// Names of the namespace's paused queues.
    pub fn list_paused_queues(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        let mut conn = self.conn()?;
        let names: Vec<String> = conn.smembers(self.paused_key(namespace)).map_err(map_err)?;
        Ok(names)
    }
}
