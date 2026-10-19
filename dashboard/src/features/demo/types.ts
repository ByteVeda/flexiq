/** One job a trigger accepted. */
export interface TriggeredJob {
  id: string;
  /** A redelivery the trigger's unique key folded into an existing job. */
  deduplicated: boolean;
}

/** A trigger's `202` body. */
export interface TriggerResponse {
  jobs: TriggeredJob[];
}
