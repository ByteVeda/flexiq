//! Notice when mounted key material changes, so a listener can reload it.
//!
//! cert-manager renews a Secret in place and certificates rotate on a 90-day
//! cadence. The kubelet swaps the projected files under a running pod, so a
//! listener that read them once would serve the certificate it booted with
//! until something restarted it — and its clients would start refusing the
//! connection the moment that certificate expired.
//!
//! Content, not mtime: a projected Secret is a symlink dance whose timestamps
//! move for reasons unrelated to the bytes, and reloading rustls needlessly is
//! worse than reading a few small files.

use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use crate::runtime::shutdown::Shutdown;

/// How often mounted key material is checked. A kubelet takes up to a minute
/// to project an updated Secret anyway, so polling faster would only burn
/// syscalls.
pub const POLL: Duration = Duration::from_secs(30);

/// Call `reload` whenever the content of any file in `paths` changes, until
/// `shutdown`.
///
/// A read failure — mid-swap, most likely — is not a change: the listener
/// keeps what it has and looks again next tick. A failed `reload` is logged and
/// retried on the next change, leaving the listener on its previous material,
/// which still works until it expires.
pub async fn watch_files<F, Fut>(
    label: &'static str,
    paths: Vec<PathBuf>,
    poll: Duration,
    shutdown: Shutdown,
    mut reload: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let mut current = read_all(&paths).await;
    loop {
        tokio::select! {
            _ = shutdown.wait() => return,
            _ = tokio::time::sleep(poll) => {}
        }

        let latest = read_all(&paths).await;
        if latest.is_none() || latest == current {
            continue;
        }

        match reload().await {
            Ok(()) => {
                log::info!("[flexiq] reloaded the {label} certificate");
                current = latest;
            }
            Err(error) => log::error!(
                "the {label} certificate changed on disk but could not be loaded: {error:#}"
            ),
        }
    }
}

/// Every file's bytes, or `None` if any of them could not be read.
async fn read_all(paths: &[PathBuf]) -> Option<Vec<Vec<u8>>> {
    let mut contents = Vec::with_capacity(paths.len());
    for path in paths {
        contents.push(tokio::fs::read(path).await.ok()?);
    }
    Some(contents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const FAST: Duration = Duration::from_millis(20);

    fn temp_file(label: &str, content: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "flexiq-watch-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, content).expect("write");
        path
    }

    async fn settle() {
        tokio::time::sleep(FAST * 5).await;
    }

    #[tokio::test]
    async fn a_content_change_reloads_once() {
        let path = temp_file("change", b"one");
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = Shutdown::default();
        let task = tokio::spawn(watch_files(
            "test",
            vec![path.clone()],
            FAST,
            shutdown.clone(),
            {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                }
            },
        ));

        settle().await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "unchanged files are not reloaded"
        );
        std::fs::write(&path, b"two").expect("rewrite");
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        shutdown.trigger();
        task.await.expect("watch");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_failed_reload_is_retried_on_the_next_tick() {
        let path = temp_file("retry", b"one");
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = Shutdown::default();
        let task = tokio::spawn(watch_files(
            "test",
            vec![path.clone()],
            FAST,
            shutdown.clone(),
            {
                let calls = Arc::clone(&calls);
                move || {
                    let attempt = calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            anyhow::bail!("half-written");
                        }
                        Ok(())
                    }
                }
            },
        ));

        // Let the watch read its baseline before the change it must notice.
        settle().await;
        std::fs::write(&path, b"two").expect("rewrite");
        settle().await;
        // The failure left `current` on the old bytes, so the same change is
        // tried again and then accepted — and then left alone.
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        shutdown.trigger();
        task.await.expect("watch");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn an_unreadable_file_is_not_a_change() {
        let path = temp_file("missing", b"one");
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = Shutdown::default();
        let task = tokio::spawn(watch_files(
            "test",
            vec![path.clone()],
            FAST,
            shutdown.clone(),
            {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                }
            },
        ));

        settle().await;
        std::fs::remove_file(&path).expect("remove");
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        shutdown.trigger();
        task.await.expect("watch");
    }
}
