import { SdkSwap } from "@/components/sdk-text";

// Worker pool — exact port of the prototype's `.archstack` + `.fork-route` +
// `.archfork` (scheduler routes by task type; sync OS-thread pool vs async pool).
// The per-runtime details adapt to the active SDK. `architecture/worker-pool.mdx`.
export function WorkerDispatch() {
  return (
    <div className="archstack">
      <div className="layer rust">
        <span className="ltag t-rust">Rust</span>
        <div className="lbody">
          <div className="lt">Scheduler</div>
          <div className="ld">
            Dequeues a job, applies rate limits, then routes it{" "}
            <b>by task type</b> — sync functions to the thread pool,{" "}
            <SdkSwap
              python={<code>async def</code>}
              node={<code>async</code>}
              java="handler"
              rust={<code>register_async</code>}
            />{" "}
            functions to the async runtime.
          </div>
        </div>
        <span className="lrole">routes by task type</span>
      </div>
      <div className="fork-route">
        <SdkSwap
          python="sync def · async def"
          node="sync · async"
          java="handlers"
          rust="#[task] · register_async"
        />
      </div>
      <div className="archfork">
        <div className="forkcol">
          <div className="forktag">bounded mpsc · workers × 2</div>
          <div className="layer py">
            <span className="ltag t-py">Sync pool</span>
            <div className="lbody">
              <div className="lt">OS-thread workers</div>
              <div className="ld">
                <SdkSwap
                  python={
                    <>
                      Each sync worker is a Rust <code>std::thread</code>. The
                      GIL is acquired per task via{" "}
                      <code>Python::with_gil()</code> — independent across
                      workers.
                    </>
                  }
                  node={
                    <>
                      Sync handlers run on an OS-thread pool owned by the Rust
                      core — independent across workers, with no event loop to
                      block.
                    </>
                  }
                  java={
                    <>
                      Handlers run on a JVM <code>ExecutorService</code> — a
                      cached pool by default, fixed with{" "}
                      <code>concurrency(n)</code> — fed by the JNI dispatch
                      bridge.
                    </>
                  }
                  rust={
                    <>
                      Each task body runs on a Tokio <code>spawn_blocking</code>{" "}
                      thread, wrapped in <code>catch_unwind</code> — a panic
                      fails the job as a retryable error instead of stranding
                      it.
                    </>
                  }
                />
              </div>
            </div>
          </div>
        </div>
        <div className="forkcol">
          <div className="forktag">bypasses the thread pool</div>
          <div className="layer py">
            <span className="ltag t-py">Async pool</span>
            <div className="lbody">
              <div className="lt">
                <SdkSwap
                  python="NativeAsyncPool"
                  node="Native async pool"
                  java="Executor dispatch"
                  rust="Worker::register_async"
                />
              </div>
              <div className="ld">
                <SdkSwap
                  python={
                    <>
                      <code>async def</code> tasks are dispatched to an{" "}
                      <code>AsyncTaskExecutor</code> on a Python daemon thread;{" "}
                      <code>PyResultSender</code> bridges results back.
                    </>
                  }
                  node={
                    <>
                      <code>async</code> handlers run on a native async pool —
                      no thread per job; each runs on your Node event loop and
                      its promise is awaited back into the core.
                    </>
                  }
                  java={
                    <>
                      Every job is an executor task; the handler's return value
                      (or exception) is bridged back into the Rust scheduler as
                      the job outcome.
                    </>
                  }
                  rust={
                    <>
                      <code>#[task]</code> refuses an <code>async fn</code>.
                      Register the future directly on{" "}
                      <code>flexiq_core::Worker</code> with{" "}
                      <code>register_async</code> — it runs on the core&apos;s
                      own runtime, with no step session.
                    </>
                  }
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
