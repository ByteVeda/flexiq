#!/usr/bin/env bash
# Enqueue work from a shell script — no SDK, no CBOR library, no language
# runtime at all — against a flexiq-server gRPC/JSON producer door.
#
# This is the same "orders.process" job producer.py enqueues, sent over the
# network instead of written straight into a shared database file. See the
# "Server mode: the gRPC variant" section of README.md.
#
# Requires grpcurl (https://github.com/fullstorydev/grpcurl) and jq.
set -euo pipefail

ADDR="${FLEXIQ_GRPC_ADDR:-localhost:50051}"
ORDERS="${1:-3}"

if [[ -z "${FLEXIQ_TOKEN:-}" ]]; then
  echo "Error: FLEXIQ_TOKEN is required — mint one with" >&2
  echo "  flexiq-server token create --name polyglot-producer --scope produce" >&2
  exit 1
fi

for cmd in grpcurl jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: $cmd is required" >&2
    exit 1
  fi
done

for ((n = 1; n <= ORDERS; n++)); do
  order_id=$(printf "ord-%04d" "$n")
  amount_cents=$((1000 * n))

  # `structured` builds the same CBOR envelope an SDK would, from plain JSON —
  # this shell has no CBOR encoder to reach for. See "raw versus structured"
  # in the server mode docs for exactly what it refuses rather than rounds.
  request=$(jq -n \
    --arg order_id "$order_id" \
    --argjson amount_cents "$amount_cents" \
    '{
      task_name: "orders.process",
      structured: {args: [{
        order_id: $order_id,
        customer: "ada@example.com",
        amount_cents: $amount_cents,
        currency: "EUR"
      }]},
      options: {queue: "process"}
    }')

  job_id=$(grpcurl -plaintext \
    -H "authorization: Bearer ${FLEXIQ_TOKEN}" \
    -d "$request" \
    "${ADDR}" flexiq.v1.ProducerService/Enqueue | jq -r '.job.id')

  echo "enqueued orders.process ${order_id} job=${job_id} (via gRPC, ${ADDR})"
done

echo
echo "${ORDERS} order(s) queued on ${ADDR}. Start the workers to drain them."
