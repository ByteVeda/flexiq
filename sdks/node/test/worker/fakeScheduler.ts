/**
 * The scheduler end of an attach, driven frame by frame.
 *
 * Shared by every test that needs a scheduler without building one: the
 * executor speaks a socket protocol, so a plain `net` server is enough to drive
 * one end of it. `executorAttachServer.test.ts` runs the same assertions
 * against the real server binary, which is what keeps this fake honest.
 */

import { createServer, type Server, type Socket } from "node:net";

/** Frame-format version this build speaks; mirrored from the core. */
export const PROTOCOL_VERSION = 1;

/** How long a test waits for a frame before calling it a failure. */
export const SETTLE_MS = 15_000;

export interface Frame {
  header: Record<string, unknown>;
  payload: Buffer;
}

/** One already-committed step, as a dispatch's snapshot carries it. */
export interface SnapshotStep {
  seq: number;
  stepKey: string;
  kind?: "run" | "sleep";
  /** The memoized bytes. Absent for a sleep, which commits none. */
  result?: Buffer;
  wakeAt?: number;
  createdAt?: number;
}

/**
 * The scheduler end of an attach, driven frame by frame.
 *
 * A frame is a JSON header line followed by exactly the number of raw payload
 * bytes it declares, so decoding has to be length-driven rather than
 * line-driven — a payload can contain newlines.
 */
export class FakeScheduler {
  private server: Server;
  private socket?: Socket;
  private buffer = Buffer.alloc(0);
  private frames: Frame[] = [];
  private waiters: (() => void)[] = [];
  private connected: Promise<void>;
  port = 0;
  /** The `hello` the executor opened with, once it has attached. */
  hello?: Record<string, unknown>;
  /** Set when the handshake should be refused rather than acked. */
  refuse = false;
  /** Optional behaviours this scheduler advertises in its `hello_ack`. */
  capabilities: readonly string[] = [];
  /** Run immediately after the ack, to dispatch into the attach window. */
  afterAck?: () => void;

  private constructor(server: Server, port: number, connected: Promise<void>) {
    this.server = server;
    this.port = port;
    this.connected = connected;
  }

  static async listen(options?: { refuse?: boolean }): Promise<FakeScheduler> {
    const server = createServer();
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (address === null || typeof address === "string") {
      throw new Error("expected a TCP address");
    }

    let onConnected: () => void = () => {};
    const connected = new Promise<void>((resolve) => {
      onConnected = resolve;
    });
    const fake = new FakeScheduler(server, address.port, connected);
    if (options?.refuse) {
      fake.refuse = true;
    }

    server.on("connection", (socket) => {
      fake.socket = socket;
      socket.on("data", (chunk: Buffer) => {
        fake.buffer = Buffer.concat([fake.buffer, chunk]);
        fake.drain();
        onConnected();
      });
      socket.on("error", () => {});
    });
    return fake;
  }

  /** Decode every whole frame currently buffered. */
  private drain(): void {
    for (;;) {
      const newline = this.buffer.indexOf(0x0a);
      if (newline < 0) {
        return;
      }
      const header = JSON.parse(this.buffer.subarray(0, newline).toString()) as Record<
        string,
        unknown
      >;
      const declared = declaredPayloadLength(header);
      const total = newline + 1 + declared;
      if (this.buffer.length < total) {
        return;
      }
      const payload = this.buffer.subarray(newline + 1, total);
      this.buffer = this.buffer.subarray(total);

      if (header.type === "hello") {
        this.hello = header;
        if (!this.refuse) {
          this.send({
            type: "hello_ack",
            schedulerId: undefined,
            scheduler_id: "fake-scheduler",
            protocol_version: PROTOCOL_VERSION,
            // Whatever this scheduler promises to do on the executor's behalf.
            // Empty by default: that is a scheduler built before the
            // side-channel existed, and the case worth defaulting to.
            capabilities: this.capabilities,
          });
          this.afterAck?.();
        } else {
          this.socket?.destroy();
        }
        continue;
      }
      this.frames.push({ header, payload: Buffer.from(payload) });
      for (const wake of this.waiters.splice(0)) {
        wake();
      }
    }
  }

  send(header: Record<string, unknown>, payload: Buffer = Buffer.alloc(0)): void {
    const clean = Object.fromEntries(Object.entries(header).filter(([, v]) => v !== undefined));
    this.socket?.write(`${JSON.stringify(clean)}\n`);
    if (payload.length > 0) {
      this.socket?.write(payload);
    }
  }

  sendJob(
    id: string,
    taskName: string,
    payload: Buffer,
    options?: {
      retryCount?: number;
      maxRetries?: number;
      timeoutMs?: number;
      disabledMiddleware?: readonly string[];
    },
  ): void {
    this.send(
      {
        type: "job",
        id,
        task_name: taskName,
        payload_len: payload.length,
        retry_count: options?.retryCount ?? 0,
        max_retries: options?.maxRetries ?? 3,
        queue: "default",
        timeout_ms: options?.timeoutMs ?? 30_000,
        namespace: null,
        // Resolved by the scheduler, because an executor has no settings store
        // of its own to read the toggle list from.
        disabled_middleware: options?.disabledMiddleware ?? [],
        metadata: null,
      },
      payload,
    );
  }

  /**
   * Send the steps a job has already committed, as the dispatch's own
   * `job_steps` frame.
   *
   * Must precede {@link sendJob} for that id: the executor decodes it on
   * arrival and keys it by job, and a snapshot that lands after the dispatch is
   * a snapshot the attempt has already replayed without. No frame at all is an
   * empty snapshot, which is why one is only sent when there is something in it.
   *
   * The payload is the core's own encoding — a JSON metadata line, then every
   * step's blob concatenated in `seq` order. A `result` inside the JSON would
   * render as an array of numbers and inflate the frame several-fold.
   */
  sendJobSteps(jobId: string, steps: readonly SnapshotStep[]): void {
    const metadata = steps.map((step) => ({
      seq: step.seq,
      step_key: step.stepKey,
      kind: step.kind ?? "run",
      result_len: step.result === undefined ? null : step.result.length,
      wake_at: step.wakeAt ?? null,
      created_at: step.createdAt ?? 0,
    }));
    const blobs = steps.map((step) => step.result ?? Buffer.alloc(0));
    const payload = Buffer.concat([Buffer.from(`${JSON.stringify(metadata)}\n`), ...blobs]);
    this.send({ type: "job_steps", job_id: jobId, payload_len: payload.length }, payload);
  }

  /** Acknowledge a `step_commit`, which is what the task is blocked on. */
  ackStep(commit: Frame, options?: { wakeAt?: number }): void {
    this.send({
      type: "step_ack",
      job_id: commit.header.job_id,
      seq: commit.header.seq,
      ok: true,
      already: false,
      wake_at: options?.wakeAt,
    });
  }

  /** Refuse a `step_commit`, with the verdict only the storage side can make. */
  refuseStep(
    commit: Frame,
    error: string,
    failure: "retryable" | "permanent" | "superseded",
  ): void {
    this.send({
      type: "step_ack",
      job_id: commit.header.job_id,
      seq: commit.header.seq,
      ok: false,
      already: false,
      error,
      failure,
    });
  }

  /** The next frame of a given type, skipping everything before it. */
  async nextFrame(type: string): Promise<Frame> {
    const deadline = Date.now() + SETTLE_MS;
    for (;;) {
      const found = this.frames.findIndex((frame) => frame.header.type === type);
      const frame = found >= 0 ? this.frames.splice(found, 1)[0] : undefined;
      if (frame !== undefined) {
        return frame;
      }
      if (Date.now() > deadline) {
        throw new Error(`no ${type} frame arrived`);
      }
      await sleep(20);
    }
  }

  /** Drop the connection without closing the listener. */
  disconnect(): void {
    this.socket?.destroy();
  }

  /**
   * Every side-channel frame a job produced, plus its result.
   *
   * The result is ordered behind them on one connection, so its arrival is
   * what proves the collection is complete rather than merely early.
   */
  async collectUntilResult(): Promise<{ result: Frame; sideChannel: Frame[] }> {
    const sideChannel: Frame[] = [];
    for (;;) {
      const frame = await this.nextResult();
      if (frame.header.type === "progress" || frame.header.type === "task_log") {
        sideChannel.push(frame);
        continue;
      }
      return { result: frame, sideChannel };
    }
  }

  /** Wait for the executor's `hello` to arrive. */
  async attached(): Promise<Record<string, unknown>> {
    await this.connected;
    const deadline = Date.now() + SETTLE_MS;
    while (this.hello === undefined && Date.now() < deadline) {
      await sleep(10);
    }
    if (this.hello === undefined) {
      throw new Error("the executor never sent a hello");
    }
    return this.hello;
  }

  /** The next frame that is not a heartbeat. */
  async nextResult(): Promise<Frame> {
    const deadline = Date.now() + SETTLE_MS;
    for (;;) {
      const found = this.frames.findIndex((frame) => frame.header.type !== "heartbeat");
      const frame = found >= 0 ? this.frames.splice(found, 1)[0] : undefined;
      if (frame !== undefined) {
        return frame;
      }
      if (Date.now() > deadline) {
        throw new Error("no result frame arrived");
      }
      await new Promise<void>((resolve) => {
        this.waiters.push(resolve);
        setTimeout(resolve, 50);
      });
    }
  }

  /** Block until the executor reports exactly `free` slots. */
  async heartbeat(free: number): Promise<void> {
    const deadline = Date.now() + SETTLE_MS;
    for (;;) {
      const found = this.frames.findIndex(
        (frame) => frame.header.type === "heartbeat" && frame.header.free_slots === free,
      );
      if (found >= 0) {
        this.frames.splice(0, found + 1);
        return;
      }
      if (Date.now() > deadline) {
        throw new Error(`no heartbeat reporting ${free} free slots`);
      }
      await sleep(20);
    }
  }

  close(): void {
    this.socket?.destroy();
    this.server.close();
  }
}

/**
 * Bytes of payload a header says follow it — the reader's framing rule.
 *
 * Read by field rather than by frame type, exactly as the core's own preamble
 * does: every frame added from now on declares `payload_len`, and the two names
 * that predate that rule are aliases. A reader that has to learn each new frame
 * desyncs the wire the first time one carries bytes it did not expect — the
 * `step_commit` case here.
 */
function declaredPayloadLength(header: Record<string, unknown>): number {
  for (const field of ["payload_len", "result_len", "extra_len"]) {
    const declared = header[field];
    if (typeof declared === "number") {
      return declared;
    }
  }
  return 0;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
