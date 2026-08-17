//! Native-async worker pool — Python-specific binding code.
//!
//! Formerly the separate `flexiq-async` crate; folded into the Python shell
//! because every line is Python-coupled (GIL, cloudpickle, the Python async
//! executor). Compiled only under the `native-async` feature.

mod permit;
mod pool;
mod result_sender;
mod task_executor;

pub use permit::PyJobPermit;
pub use pool::NativeAsyncPool;
pub use result_sender::PyResultSender;
