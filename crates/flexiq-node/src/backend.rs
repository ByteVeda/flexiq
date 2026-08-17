//! Feature-gated construction of a [`StorageBackend`] from JS [`OpenOptions`].

use flexiq_core::{SqliteStorage, StorageBackend};
use napi::bindgen_prelude::{Error, Result, Status};

use crate::config::OpenOptions;
use crate::error::{invalid_arg, to_napi_err};

const DEFAULT_SQLITE_POOL: u32 = 8;
#[cfg(feature = "postgres")]
const DEFAULT_POSTGRES_POOL: u32 = 10;
#[cfg(feature = "postgres")]
const DEFAULT_POSTGRES_SCHEMA: &str = "flexiq";

/// Resolve a connection pool size, rejecting an explicit zero — r2d2 requires
/// `max_size > 0` and panics otherwise, which would crash the whole process.
fn resolve_pool_size(pool_size: Option<u32>, default: u32) -> Result<u32> {
    match pool_size {
        Some(0) => Err(invalid_arg("poolSize must be greater than 0")),
        Some(n) => Ok(n),
        None => Ok(default),
    }
}

/// Open the storage backend named by `options.backend` (default `"sqlite"`).
/// Returns an error if a requested backend was not compiled into this addon.
pub fn open(options: &OpenOptions) -> Result<StorageBackend> {
    let auto_migrate = options.auto_migrate.unwrap_or(true);
    match options.backend.as_deref().unwrap_or("sqlite") {
        "sqlite" => {
            let pool = resolve_pool_size(options.pool_size, DEFAULT_SQLITE_POOL)?;
            let storage = if auto_migrate {
                SqliteStorage::with_pool_size(&options.dsn, pool)
            } else {
                SqliteStorage::unmigrated(&options.dsn, pool)
            }
            .map_err(to_napi_err)?;
            Ok(StorageBackend::Sqlite(storage))
        }
        #[cfg(feature = "postgres")]
        "postgres" => {
            let schema = options.schema.as_deref().unwrap_or(DEFAULT_POSTGRES_SCHEMA);
            let pool = resolve_pool_size(options.pool_size, DEFAULT_POSTGRES_POOL)?;
            let storage = if auto_migrate {
                flexiq_core::PostgresStorage::with_schema_and_pool_size(&options.dsn, schema, pool)
            } else {
                flexiq_core::PostgresStorage::unmigrated(&options.dsn, schema, pool)
            }
            .map_err(to_napi_err)?;
            Ok(StorageBackend::Postgres(storage))
        }
        #[cfg(feature = "redis")]
        "redis" => {
            let storage = match options.prefix.as_deref() {
                Some(prefix) => flexiq_core::RedisStorage::with_prefix(&options.dsn, prefix),
                None => flexiq_core::RedisStorage::new(&options.dsn),
            }
            .map_err(to_napi_err)?;
            Ok(StorageBackend::Redis(storage))
        }
        other => Err(Error::new(
            Status::InvalidArg,
            format!("backend '{other}' is not available (not a known backend, or this addon was built without its cargo feature)"),
        )),
    }
}
