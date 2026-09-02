import { queryOptions, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ApiError } from "@/lib/api-client";
import { createGrpcToken, listGrpcScopes, listGrpcTokens, revokeGrpcToken } from "./api";
import type { CreateGrpcTokenInput } from "./types";

const KEY = ["grpc-tokens"] as const;
const SCOPES_KEY = ["grpc-tokens", "scopes"] as const;

function describeError(error: unknown): string | undefined {
  if (error instanceof ApiError && error.status >= 400 && error.status < 500) {
    return error.message;
  }
  return undefined;
}

export function grpcTokensQuery() {
  return queryOptions({
    queryKey: KEY,
    queryFn: ({ signal }) => listGrpcTokens(signal),
  });
}

export function grpcScopesQuery() {
  return queryOptions({
    queryKey: SCOPES_KEY,
    queryFn: ({ signal }) => listGrpcScopes(signal),
    staleTime: 5 * 60 * 1000,
  });
}

export function useGrpcTokens() {
  return useQuery(grpcTokensQuery());
}

export function useGrpcScopes() {
  return useQuery(grpcScopesQuery());
}

/**
 * Whether this server has a gRPC door at all.
 *
 * `undefined` while unknown, so the nav stays quiet rather than flashing an
 * entry it may be about to remove. The probe shares the listing query, so
 * showing the page costs no extra request.
 */
export function useGrpcTokensSupported(): boolean | undefined {
  const { data, isSuccess } = useQuery({ ...grpcTokensQuery(), refetchInterval: false });
  if (!isSuccess) return undefined;
  return data !== null;
}

export function useCreateGrpcToken() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: CreateGrpcTokenInput) => createGrpcToken(input),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: KEY });
      toast.success("Token created");
    },
    onError: (error) =>
      toast.error("Failed to create token", { description: describeError(error) }),
  });
}

export function useRevokeGrpcToken() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => revokeGrpcToken(id),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: KEY });
      toast.success("Token revoked", {
        description: "It stops working on the next call. No restart is needed.",
      });
    },
    onError: (error) =>
      toast.error("Failed to revoke token", { description: describeError(error) }),
  });
}
