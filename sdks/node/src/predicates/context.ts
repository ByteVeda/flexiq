/** What a predicate sees when a job is being enqueued. */
export interface PredicateContext {
  readonly taskName: string;
  /** The positional args (after any `onEnqueue` rewrites). */
  readonly args: readonly unknown[];
  /**
   * Wall clock, read once per evaluation. Time-based recipes read this instead
   * of calling `new Date()` themselves, so a gate can be unit-tested against a
   * pinned instant (the Python SDK's `ctx.now()` serves the same purpose).
   */
  readonly now: Date;
}
