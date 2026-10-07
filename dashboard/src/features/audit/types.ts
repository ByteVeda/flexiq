/** What a record's `token_id` names. */
export type PrincipalKind = "token" | "user" | "cli" | "anonymous";

/** One recorded action, as `GET /api/audit-records` returns it. */
export interface AuditRecord {
  id: string;
  /** Unix ms the action was answered. */
  at: number;
  principal_kind: PrincipalKind;
  /** Token id for a `token`, username for a `user`; empty for `cli`/`anonymous`. */
  token_id: string;
  /** The token's name or the username at the time. */
  principal: string;
  /** RPC name, or `dashboard <METHOD> <route>` for a dashboard action. */
  operation: string;
  target_kind: string | null;
  target: string | null;
  /** gRPC status code name (`OK`, `PERMISSION_DENIED`, …). */
  outcome: string;
}

export interface AuditPage {
  records: AuditRecord[];
  /** Pass back as `after` for the next page; `null` on the last one. */
  next_cursor: string | null;
}

/** The filters the page and the URL carry. Times are Unix ms. */
export interface AuditFilters {
  tokenId?: string;
  principalKind?: PrincipalKind;
  targetKind?: string;
  target?: string;
  since?: number;
  until?: number;
}
