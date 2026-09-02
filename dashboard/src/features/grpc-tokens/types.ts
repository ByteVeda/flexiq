/** The lifecycle state the server computed for a token. */
export type GrpcTokenStatus = "active" | "expired" | "revoked";

/** A scope a token may be granted, as this build's server spells it. */
export interface GrpcScope {
  name: string;
}

/**
 * One stored gRPC API token.
 *
 * Every timestamp is Unix milliseconds, like the rest of this API. The token
 * itself is never here: it is returned once, by {@link CreatedGrpcToken}, and
 * the server keeps only its hash.
 */
export interface GrpcToken {
  id: string;
  name: string;
  scopes: string[];
  namespace: string;
  created_at: number;
  created_by: string | null;
  last_used_at: number | null;
  expires_at: number;
  revoked_at: number | null;
  status: GrpcTokenStatus;
}

/** A create response — the one place the token string appears. */
export interface CreatedGrpcToken extends GrpcToken {
  token: string;
}

export interface CreateGrpcTokenInput {
  name: string;
  scopes: string[];
  expires_in_days?: number;
}
