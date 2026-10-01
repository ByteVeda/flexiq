//! The `reqwest::dns::Resolve` implementation every dispatch client resolves
//! through.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::thread;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::sync::{oneshot, Semaphore};

use super::egress::{EgressPolicy, EgressRefusal};

/// Lookups one resolver runs at once. Stands in for the cap the blocking pool
/// used to impose: a wedged resolver pins at most this many threads, each for
/// at most the platform resolver's own ceiling. Dispatch concurrency is
/// bounded well below this, so a healthy resolver never queues on it.
const MAX_CONCURRENT_LOOKUPS: usize = 32;

/// A blocking name lookup; the seam tests use to wedge one.
type Lookup = Arc<dyn Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync>;

/// The resolver every dispatch client resolves through, for a *name*.
///
/// Vetting inside resolution is what closes the rebinding window for a name:
/// the addresses the connector receives for it are the addresses that just
/// passed, so there is no second lookup between the check and the socket.
/// This has nothing to say about an IP-literal host — the connector dials
/// that directly without ever calling a `Resolve` impl — which is why
/// [`EgressPolicy::permits_host`] applies the unconditional refusals itself
/// rather than leaving every case to this resolver.
///
/// **The lookup runs on a detached thread, not `spawn_blocking`.** A started
/// `getaddrinfo` cannot be cancelled, and dropping a runtime waits for every
/// task on its blocking pool — so a wedged lookup there held worker shutdown
/// open past `HttpTargetConfig::shutdown_drain`. The runtime does not wait
/// for a thread it does not own, and neither does process exit.
pub(crate) struct PinnedResolver {
    policy: Arc<EgressPolicy>,
    lookup: Lookup,
    slots: Arc<Semaphore>,
}

impl PinnedResolver {
    /// Resolve through `policy`, which every lookup is vetted against.
    pub(crate) fn new(policy: Arc<EgressPolicy>) -> Self {
        Self::with_lookup(policy, Arc::new(system_lookup), MAX_CONCURRENT_LOOKUPS)
    }

    fn with_lookup(policy: Arc<EgressPolicy>, lookup: Lookup, max_concurrent: usize) -> Self {
        Self {
            policy,
            lookup,
            slots: Arc::new(Semaphore::new(max_concurrent)),
        }
    }
}

impl Resolve for PinnedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = Arc::clone(&self.policy);
        let lookup = Arc::clone(&self.lookup);
        let slots = Arc::clone(&self.slots);
        let host = name.as_str().to_string();
        Box::pin(async move {
            let resolved = lookup_detached(lookup, slots, &host)
                .await
                .map_err(|reason| resolve_failure(&host, reason))?;
            let vetted = policy.vet(&host, resolved).map_err(refusal)?;
            Ok(Box::new(vetted.into_iter()) as Addrs)
        })
    }
}

/// Run `lookup` for `host` on its own thread and await the answer. Dropping
/// this future abandons the lookup; the thread finishes on its own.
async fn lookup_detached(
    lookup: Lookup,
    slots: Arc<Semaphore>,
    host: &str,
) -> Result<Vec<SocketAddr>, String> {
    // Taken before the spawn, so waiting for a slot stays cancellable; the
    // thread holds it until the lookup returns, wedged or not.
    let permit = slots
        .acquire_owned()
        .await
        .map_err(|closed| closed.to_string())?;
    let (answer_tx, answer_rx) = oneshot::channel();
    let thread_host = host.to_string();
    thread::Builder::new()
        .name("flexiq-dns".to_string())
        .spawn(move || {
            let _permit = permit;
            // A failed send means the caller was dropped; nobody is left to
            // tell, and the answer is simply discarded.
            let _ = answer_tx.send(lookup(&thread_host));
        })
        .map_err(|spawn_error| format!("failed to spawn lookup thread: {spawn_error}"))?;
    match answer_rx.await {
        Ok(answer) => answer.map_err(|io_error| io_error.to_string()),
        // The sender only drops unsent if the lookup panicked.
        Err(_) => Err("lookup thread exited without an answer".to_string()),
    }
}

/// `getaddrinfo`, through std. Port 0 is correct — reqwest overrides it with
/// the URL's port or the scheme default once resolution returns.
fn system_lookup(host: &str) -> io::Result<Vec<SocketAddr>> {
    (host, 0u16).to_socket_addrs().map(Iterator::collect)
}

/// A lookup that never reached the policy: the resolver itself failed, or the
/// lookup thread never answered. Distinct from [`refusal`] — this means
/// nobody tried anything, the network or the platform just did not answer.
fn resolve_failure(host: &str, reason: String) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(EgressRefusal::Resolve {
        host: host.to_string(),
        reason,
    })
}

/// A lookup the policy itself refused.
fn refusal(refusal: EgressRefusal) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(refusal)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc as std_mpsc, Mutex};
    use std::time::Duration;

    use crate::net::Allowlist;

    use super::*;

    fn policy(entries: &str, allow_loopback: bool) -> Arc<EgressPolicy> {
        Arc::new(EgressPolicy::new(
            Allowlist::parse(entries).expect("test allowlist parses"),
            allow_loopback,
        ))
    }

    /// `Addrs` (`Box<dyn Iterator<..> + Send>`) implements neither `Debug`
    /// nor `Clone`, so `unwrap_err`/`expect_err` cannot be called on the
    /// `Result` `resolve` returns — this unwraps by hand instead.
    fn expect_refusal(
        result: Result<Addrs, Box<dyn std::error::Error + Send + Sync>>,
    ) -> EgressRefusal {
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected the lookup to be refused"),
        };
        *error
            .downcast::<EgressRefusal>()
            .expect("a refused lookup carries an EgressRefusal")
    }

    #[tokio::test]
    async fn localhost_is_refused_by_a_policy_that_does_not_allow_it() {
        // A public network that has nothing to do with loopback: no network
        // access needed, `localhost` resolves locally without touching DNS.
        let resolver = PinnedResolver::new(policy("93.184.216.0/24", false));
        let name = "localhost"
            .parse::<Name>()
            .expect("localhost is a valid DNS name");

        let refusal = expect_refusal(resolver.resolve(name).await);

        // `allow_loopback` is false, so loopback is refused unconditionally
        // and deterministically — asserting the specific variant (rather
        // than also accepting `NotAllowed`) is itself a regression test for
        // the taxonomy `EgressPolicy::refusal_for` computes.
        assert!(matches!(refusal, EgressRefusal::NeverRoutable { .. }));
    }

    #[tokio::test]
    async fn localhost_resolves_when_the_policy_permits_it() {
        let resolver = PinnedResolver::new(policy("127.0.0.0/8,::1/128", true));
        let name = "localhost"
            .parse::<Name>()
            .expect("localhost is a valid DNS name");

        let addrs: Vec<SocketAddr> = resolver
            .resolve(name)
            .await
            .expect("a policy naming loopback and allowing it must resolve localhost")
            .collect();

        assert!(!addrs.is_empty());
        for addr in addrs {
            assert!(
                addr.ip().is_loopback(),
                "{addr} must be loopback, localhost resolves to nothing else"
            );
        }
    }

    #[tokio::test]
    async fn a_name_that_does_not_resolve_reports_a_resolve_error() {
        // `.invalid` is reserved by RFC 6761 as guaranteed not to resolve —
        // this still sends a real query (this is not the loopback-only
        // shortcut the other two tests get), but the assertion holds however
        // it fails: NXDOMAIN, a timeout, or any other lookup error all reach
        // this same `EgressRefusal::Resolve` arm. Do not "fix" a slow run in
        // a network-isolated sandbox by weakening this to a lighter check —
        // a slow failure here is still a correct one.
        let resolver = PinnedResolver::new(policy("93.184.216.0/24", false));
        let name = "nothing.invalid"
            .parse::<Name>()
            .expect("a valid DNS name syntactically");

        let refusal = expect_refusal(resolver.resolve(name).await);

        assert!(matches!(refusal, EgressRefusal::Resolve { .. }));
    }

    /// A lookup that blocks until the returned sender is dropped, counting
    /// its calls and signalling each start on `started`.
    fn wedged_lookup(
        calls: Arc<AtomicUsize>,
        started: std_mpsc::Sender<()>,
    ) -> (Lookup, std_mpsc::Sender<()>) {
        let (release_tx, release_rx) = std_mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        let lookup: Lookup = Arc::new(move |_host| {
            calls.fetch_add(1, Ordering::SeqCst);
            let _ = started.send(());
            // Blocks until the test drops `release_tx`, then every later
            // call returns at once on the disconnected channel.
            let _ = release_rx.lock().expect("release lock").recv();
            Err(io::Error::other("released"))
        });
        (lookup, release_tx)
    }

    fn wedged_name() -> Name {
        "wedged.example"
            .parse::<Name>()
            .expect("a valid DNS name syntactically")
    }

    #[test]
    fn a_wedged_lookup_does_not_hold_runtime_shutdown() {
        // Regression for #945: on `spawn_blocking`, dropping the runtime
        // waited for the wedged lookup — forever, here.
        let (started_tx, started_rx) = std_mpsc::channel();
        let (lookup, release_tx) = wedged_lookup(Arc::new(AtomicUsize::new(0)), started_tx);
        let resolver = PinnedResolver::with_lookup(
            policy("93.184.216.0/24", false),
            lookup,
            MAX_CONCURRENT_LOOKUPS,
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime builds");
        runtime.spawn(async move {
            let _ = resolver.resolve(wedged_name()).await;
        });
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the lookup starts");

        // Dropped off-thread so a regression fails here instead of hanging.
        let (dropped_tx, dropped_rx) = std_mpsc::channel();
        thread::spawn(move || {
            drop(runtime);
            let _ = dropped_tx.send(());
        });
        let dropped = dropped_rx.recv_timeout(Duration::from_secs(5));
        drop(release_tx);

        dropped.expect("dropping the runtime must not wait for a wedged lookup");
    }

    #[tokio::test]
    async fn lookups_past_the_cap_wait_for_a_slot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = std_mpsc::channel();
        let (lookup, release_tx) = wedged_lookup(Arc::clone(&calls), started_tx);
        let resolver = Arc::new(PinnedResolver::with_lookup(
            policy("93.184.216.0/24", false),
            lookup,
            1,
        ));

        let first = tokio::spawn({
            let resolver = Arc::clone(&resolver);
            async move { expect_refusal(resolver.resolve(wedged_name()).await) }
        });
        tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .expect("waiter joins")
            .expect("the first lookup starts");

        // The only slot is held by the wedged lookup: a second one queues,
        // and abandoning it while queued starts no thread.
        let queued =
            tokio::time::timeout(Duration::from_millis(200), resolver.resolve(wedged_name())).await;
        assert!(
            queued.is_err(),
            "a lookup past the cap must wait for a slot"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Releasing the wedge frees the slot for the next lookup.
        drop(release_tx);
        assert!(matches!(
            first.await.expect("first lookup joins"),
            EgressRefusal::Resolve { .. }
        ));
        let next = expect_refusal(resolver.resolve(wedged_name()).await);
        assert!(matches!(next, EgressRefusal::Resolve { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
