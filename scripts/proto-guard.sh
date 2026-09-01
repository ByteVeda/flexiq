#!/usr/bin/env bash
# Proves the proto gate catches what it claims to.
#
# Every case is staged next to a copy of contracts/proto/buf.yaml — the
# production config itself — so relaxing `lint.use` to BASIC or `breaking.use`
# to WIRE turns this red. The fixtures test our configuration, not buf.
#
# See contracts/proto-guard/README.md for what each case pins.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
guard="$repo_root/contracts/proto-guard"
config="$repo_root/contracts/proto/buf.yaml"

if ! command -v buf >/dev/null 2>&1; then
  echo "error: buf is not on PATH — see scripts/proto-check.sh for the install line." >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Copy a fixture tree into the workdir and drop the real buf.yaml beside it.
stage() {
  # Separate declarations: bash expands a whole `local a=… b=$a` line before
  # the builtin runs, so `$2`'s value is not visible to a sibling assignment.
  local src="$1"
  local dest="$work/$2"
  mkdir -p "$dest"
  cp -R "$src/." "$dest/"
  cp "$config" "$dest/buf.yaml"
  printf '%s' "$dest"
}

baseline="$(stage "$guard/baseline" baseline)"
buf build "$baseline" -o "$work/baseline.binpb"

# case | check | expected outcome
cases=(
  "renumbered-field|breaking|fail"
  "removed-field-bare|breaking|fail"
  "removed-field-number-only|breaking|fail"
  "removed-field-reserved|breaking|pass"
  "renamed-field|breaking|fail"
  "added-field|breaking|pass"
  "service-suffix|lint|fail"
  "enum-value-prefix|lint|fail"
)

failures=0

for entry in "${cases[@]}"; do
  IFS='|' read -r name check expected <<<"$entry"
  dir="$(stage "$guard/cases/$name" "$name")"

  set +e
  case "$check" in
    breaking) output="$(buf breaking "$dir" --against "$work/baseline.binpb" 2>&1)" ;;
    lint) output="$(buf lint "$dir" 2>&1)" ;;
    *)
      echo "error: unknown check '$check' for case '$name'" >&2
      exit 2
      ;;
  esac
  status=$?
  set -e

  actual=pass
  [ "$status" -ne 0 ] && actual=fail

  if [ "$actual" = "$expected" ]; then
    printf '  ok    %-26s %-8s %s\n' "$name" "$check" "$expected"
  else
    printf '  FAIL  %-26s %-8s expected %s, got %s\n' "$name" "$check" "$expected" "$actual"
    # buf prints its findings on the failing side only; on an unexpected pass
    # there is nothing to show and the absence is the finding.
    [ -n "$output" ] && printf '        %s\n' "$output"
    failures=$((failures + 1))
  fi
done

if [ "$failures" -ne 0 ]; then
  echo
  echo "error: $failures guard case(s) disagreed with contracts/proto/buf.yaml." >&2
  echo "error: the proto gate is not enforcing what §11 of the design doc requires." >&2
  exit 1
fi

echo
echo "proto-guard: all ${#cases[@]} cases behaved as configured."
