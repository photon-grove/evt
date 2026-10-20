#!/usr/bin/env bash
# Cross-language integration run: the Go and Rust drivers take turns writing the same streams
# (each scenario step names its writer), then both verify every stream against the golden
# fixtures. Usage: scripts/compat-integration.sh dynamo|postgres
set -euo pipefail

backend="${1:?usage: compat-integration.sh dynamo|postgres}"
run="${EVT_COMPAT_RUN:-r$(date +%s)${RANDOM}}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

mkdir -p target/compat
go build -o "target/compat/evt-compat-go-${backend}" ./internal/compat/cmd/evt-compat
cargo build --quiet -p evt-compat --features drivers --bin evt-compat

go_driver="target/compat/evt-compat-go-${backend}"
rust_driver="target/debug/evt-compat"

steps="$("$go_driver" steps)"
echo "run ${run}: ${steps} interleaved steps against ${backend}"

for ((index = 0; index < steps; index++)); do
  "$go_driver" step -backend "$backend" -run "$run" -index "$index"
  "$rust_driver" step --backend "$backend" --run "$run" --index "$index"
done

# An exclusive scenario holds store-wide state across its steps (a rebuild lock refuses every other
# scenario's commits), so it runs alone, after the interleaved scenarios, one step at a time.
# A failed exclusive step must not leave its rebuild lock behind for the next run: settle the fence.
trap '"$go_driver" settle -backend "$backend" || true' ERR
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

"$go_driver" verify -backend "$backend" -run "$run"
"$rust_driver" verify --backend "$backend" --run "$run"

# Event-log enumeration (P3): both languages must list the same events, unfiltered and by type.
"$go_driver" enumerate -backend "$backend" -run "$run" >"target/compat/enumerate-go-${backend}.txt"
"$rust_driver" enumerate --backend "$backend" --run "$run" >"target/compat/enumerate-rust-${backend}.txt"
if [ ! -s "target/compat/enumerate-go-${backend}.txt" ]; then
  echo "run ${run}: the Go enumeration of ${backend} was empty" >&2
  exit 1
fi
diff -u "target/compat/enumerate-go-${backend}.txt" "target/compat/enumerate-rust-${backend}.txt"

echo "run ${run}: Go and Rust agree on every ${backend} stream and on its enumeration"
