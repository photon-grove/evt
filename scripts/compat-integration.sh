#!/usr/bin/env bash
# Cross-language integration run: the Go and Rust drivers take turns writing the same streams
# (each scenario step names its writer), then both verify every stream against the golden
# fixtures. On DynamoDB, two passes over the scenarios the TypeScript driver supports come first, in
# which it stands in for Go and then for Rust (ADR 0009). Usage: scripts/compat-integration.sh
# dynamo|postgres
set -euo pipefail

backend="${1:?usage: compat-integration.sh dynamo|postgres}"
run="${EVT_COMPAT_RUN:-r$(date +%s)${RANDOM}}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

mkdir -p target/compat
go -C go build -o "$root/target/compat/evt-compat-go-${backend}" ./internal/compat/cmd/evt-compat
cargo build --quiet -p evt-compat --features drivers --bin evt-compat

go_driver="target/compat/evt-compat-go-${backend}"
rust_driver="target/debug/evt-compat"

# The TypeScript driver writes only the DynamoDB run so far.
ts_driver=(node packages/evt-compat/dist/live.js)
ts_scenarios=""
if [ "$backend" = dynamo ]; then
  pnpm --silent --filter @photon-grove/evt run build
  pnpm --silent --filter @photon-grove/evt-compat run build
  ts_scenarios="$("${ts_driver[@]}" scenarios)"
  if [ -z "$ts_scenarios" ]; then
    echo "the TypeScript driver lists no scenarios" >&2
    exit 1
  fi
fi

# pass RUN [go|rust]: one cross-language run under its own run ID. With a writer name, the
# TypeScript driver writes that language's steps, and the pass takes only the scenarios TypeScript
# supports: the others would only repeat the first pass, and the emulator slows as its table grows.
pass() {
  local run="$1" as="${2:-}" listed="" label="Go and Rust" scope="every ${backend} stream"
  case "$as" in
    go) listed="$ts_scenarios" label="TypeScript (as Go) and Rust" ;;
    rust) listed="$ts_scenarios" label="Go and TypeScript (as Rust)" ;;
  esac
  if [ -n "$as" ]; then
    scope="the ${backend} streams of the scenarios TypeScript supports"
  fi

  local steps
  steps="$("$go_driver" steps)"
  echo "run ${run}: ${steps} interleaved steps against ${backend}, written by ${label}"

  for ((index = 0; index < steps; index++)); do
    if [ "$as" = go ]; then
      "${ts_driver[@]}" step --backend "$backend" --run "$run" --index "$index" --as go
    else
      "$go_driver" step -backend "$backend" -run "$run" -index "$index" -scenarios "$listed"
    fi
    if [ "$as" = rust ]; then
      "${ts_driver[@]}" step --backend "$backend" --run "$run" --index "$index" --as rust
    else
      "$rust_driver" step --backend "$backend" --run "$run" --index "$index" --scenarios "$listed"
    fi
  done

  # An exclusive scenario holds store-wide state across its steps (a rebuild lock refuses every
  # other scenario's commits), so it runs alone, after the interleaved scenarios, one step at a
  # time. TypeScript supports none yet, so only the first pass runs them. A failed exclusive step
  # must not leave its rebuild lock or a declared feature behind for the next run: settle the fence
  # and the feature marker.
  if [ -z "$as" ]; then
    trap '"$go_driver" settle -backend "$backend" || true' ERR
    local exclusive_scenarios scenario exclusive_steps
    exclusive_scenarios="$("$go_driver" exclusive)"
    while IFS= read -r scenario; do
      [ -n "$scenario" ] || continue
      exclusive_steps="$("$go_driver" steps -only "$scenario")"
      echo "run ${run}: ${exclusive_steps} exclusive steps of ${scenario} against ${backend}"
      for ((index = 0; index < exclusive_steps; index++)); do
        "$go_driver" step -backend "$backend" -run "$run" -index "$index" -only "$scenario"
        "$rust_driver" step --backend "$backend" --run "$run" --index "$index" --only "$scenario"
      done
    done <<<"$exclusive_scenarios"
    trap - ERR
  fi

  "$go_driver" verify -backend "$backend" -run "$run" -scenarios "$listed"
  "$rust_driver" verify --backend "$backend" --run "$run" --scenarios "$listed"
  if [ -n "$as" ]; then
    "${ts_driver[@]}" verify --backend "$backend" --run "$run"
  fi

  # Event-log enumeration (P3): both languages must list the same events, unfiltered and by type.
  local go_list="target/compat/enumerate-go-${backend}.txt"
  local rust_list="target/compat/enumerate-rust-${backend}.txt"
  "$go_driver" enumerate -backend "$backend" -run "$run" >"$go_list"
  "$rust_driver" enumerate --backend "$backend" --run "$run" >"$rust_list"
  if [ ! -s "$go_list" ]; then
    echo "run ${run}: the Go enumeration of ${backend} was empty" >&2
    exit 1
  fi
  diff -u "$go_list" "$rust_list"

  echo "run ${run}: ${label} agree on ${scope} and on their enumeration"
}

# The TypeScript passes are small, so they go first, while the emulator's tables are smallest: a
# transaction in Moto copies its table, so every pass slows the ones after it. Each pass writes its
# own streams. Enumeration matches the run ID anywhere in an entity ID, so the last pass also lists
# the TypeScript passes' events, which Go and Rust must list alike.
if [ -n "$ts_scenarios" ]; then
  pass "ts-go-${run}" go
  pass "ts-rust-${run}" rust
fi

pass "$run"
