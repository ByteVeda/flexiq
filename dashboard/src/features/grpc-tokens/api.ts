import { ApiError, api } from "@/lib/api-client";
import type { CreatedGrpcToken, CreateGrpcTokenInput, GrpcScope, GrpcToken } from "./types";

/**
 * Fetch the stored tokens, or `null` when this server has no gRPC door.
 *
 * Only `flexiq-server` serves this route; an SDK dashboard answers 404.
 * Returning `null` rather than throwing is what lets the nav hide the page
 * instead of surfacing an error the operator can do nothing about — the same
 * shape `/api/executors` uses.
 */
export async function listGrpcTokens(signal?: AbortSignal): Promise<GrpcToken[] | null> {
  try {
    return await api.get<GrpcToken[]>("/api/grpc-tokens", { signal });
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}

/** The scopes this server understands, so the dialog offers what it can grant. */
export function listGrpcScopes(signal?: AbortSignal): Promise<GrpcScope[]> {
  return api.get<GrpcScope[]>("/api/grpc-tokens/scopes", { signal });
}

export function createGrpcToken(input: CreateGrpcTokenInput): Promise<CreatedGrpcToken> {
  return api.post<CreatedGrpcToken>("/api/grpc-tokens", input);
}

export function revokeGrpcToken(id: string): Promise<{ id: string; revoked: true }> {
  return api.delete<{ id: string; revoked: true }>(`/api/grpc-tokens/${encodeURIComponent(id)}`);
}
