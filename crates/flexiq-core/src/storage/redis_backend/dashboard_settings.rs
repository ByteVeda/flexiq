use std::collections::HashMap;

use redis::Commands;

use super::{map_err, RedisStorage};
use crate::error::Result;

/// Redis key for the dashboard settings hash. All keys are stored under
/// a single hash so atomic ``HGETALL`` returns the full snapshot.
fn settings_key(storage: &RedisStorage) -> String {
    storage.key(&["dashboard", "settings"])
}

/// Lua script: write a hash field only if it still holds the expected value.
/// `ARGV[2]` is `1` when a value is expected and `0` when the field must be
/// unset — an empty string is a legitimate stored value, so absence needs its
/// own flag rather than a sentinel.
const COMPARE_AND_SET_SCRIPT: &str = r#"
    local current = redis.call('HGET', KEYS[1], ARGV[1])
    if ARGV[2] == '1' then
        if current == false or current ~= ARGV[3] then
            return 0
        end
    elseif current ~= false then
        return 0
    end
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
    return 1
"#;

impl RedisStorage {
    /// Fetch a single setting value by key, or `None` if unset.
    pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.conn()?;
        let value: Option<String> = conn.hget(settings_key(self), key).map_err(map_err)?;
        Ok(value)
    }

    /// Insert or update a setting.
    pub fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let mut conn = self.conn()?;
        conn.hset::<_, _, _, ()>(settings_key(self), key, value)
            .map_err(map_err)?;
        Ok(())
    }

    /// Write a setting only if it still holds `expected`. `None` means the key
    /// must be unset.
    pub fn set_setting_if(&self, key: &str, expected: Option<&str>, value: &str) -> Result<bool> {
        let mut conn = self.conn()?;
        let written: i32 = redis::Script::new(COMPARE_AND_SET_SCRIPT)
            .key(settings_key(self))
            .arg(key)
            .arg(i32::from(expected.is_some()))
            .arg(expected.unwrap_or_default())
            .arg(value)
            .invoke(&mut conn)
            .map_err(map_err)?;
        Ok(written == 1)
    }

    /// Delete a setting. Returns `true` if an entry was removed.
    pub fn delete_setting(&self, key: &str) -> Result<bool> {
        let mut conn = self.conn()?;
        let removed: i64 = conn.hdel(settings_key(self), key).map_err(map_err)?;
        Ok(removed > 0)
    }

    /// All settings as a key-to-value map.
    pub fn list_settings(&self) -> Result<HashMap<String, String>> {
        let mut conn = self.conn()?;
        let map: HashMap<String, String> = conn.hgetall(settings_key(self)).map_err(map_err)?;
        Ok(map)
    }
}
