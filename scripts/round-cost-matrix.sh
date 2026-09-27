#!/usr/bin/env bash
#
# Round-cost matrix.
#
# Runs Kommander.Benchmark once per arm and appends one JSON line per arm, then fits
# round = a + b·n over the batch-size arms of each group and appends the fits. One command,
# one host: re-run it on a Kommander change and compare the fits.
#
# Each arm is its own process, one after the other. Do not run this at the same time as
# `dotnet test` or another benchmark: both are timing-sensitive and share the CPU.
#
# Usage:
#   scripts/round-cost-matrix.sh [output.jsonl]
#
# Environment (defaults in brackets):
#   TRANSPORTS    ["grpc-mtls grpc-plaintext memory"]
#   STORAGES      ["memory rocksdb"]
#   BATCHES       ["1 16 64 256"]
#   CONCURRENCY   ["1 128"]
#   PAYLOAD       [280]
#   DURATION      [20s]   measurement window per arm
#   WARMUP        [5s]
#   WAL_DIR       [/dev/shm if present, else the OS temp path]  parent of the RocksDB directories
#   SYNC_WRITES   [false]  fsync for rocksdb; false keeps fsync out like the tmpfs CamusDB runs
#   LABEL         [git short sha]
#   FRAMEWORK     [net10.0]
#
# On Linux, /dev/shm is tmpfs, so SYNC_WRITES=true there still has a free fsync. macOS has no
# tmpfs; SYNC_WRITES=false writes the RocksDB WAL to the page cache without a sync, which is the
# nearest equivalent.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OUTPUT="${1:-round-cost-$(date -u +%Y%m%dT%H%M%SZ).jsonl}"
TRANSPORTS="${TRANSPORTS:-grpc-mtls grpc-plaintext memory}"
STORAGES="${STORAGES:-memory rocksdb}"
BATCHES="${BATCHES:-1 16 64 256}"
CONCURRENCY="${CONCURRENCY:-1 128}"
PAYLOAD="${PAYLOAD:-280}"
DURATION="${DURATION:-20s}"
WARMUP="${WARMUP:-5s}"
SYNC_WRITES="${SYNC_WRITES:-false}"
FRAMEWORK="${FRAMEWORK:-net10.0}"
LABEL="${LABEL:-$(git rev-parse --short HEAD 2>/dev/null || echo local)$(git diff --quiet 2>/dev/null || echo "+dirty")}"

if [[ -z "${WAL_DIR:-}" ]]; then
  if [[ -d /dev/shm ]]; then WAL_DIR=/dev/shm; else WAL_DIR="${TMPDIR:-/tmp}"; fi
fi

if pgrep -f "dotnet test|testhost" >/dev/null 2>&1; then
  echo "A dotnet test run is active. Wait for it to finish: the numbers would be wrong." >&2
  exit 1
fi

dotnet build Kommander.Benchmark/Kommander.Benchmark.csproj -c Release -f "$FRAMEWORK" -nologo -v quiet >/dev/null
BENCH="Kommander.Benchmark/bin/Release/$FRAMEWORK/Kommander.Benchmark.dll"

echo "Output: $OUTPUT   label: $LABEL   wal-dir: $WAL_DIR   sync: $SYNC_WRITES"

port=52000
for transport in $TRANSPORTS; do
  case "$transport" in
    grpc-mtls)      transport_args=(--transport grpc) ;;
    grpc-plaintext) transport_args=(--transport grpc --plaintext) ;;
    memory)         transport_args=(--transport memory) ;;
    *) echo "Unknown transport $transport" >&2; exit 2 ;;
  esac

  for storage in $STORAGES; do
    for concurrency in $CONCURRENCY; do
      for batch in $BATCHES; do
        echo "── transport=$transport storage=$storage concurrency=$concurrency batch=$batch"

        # A fresh port range per arm: a previous arm's sockets can sit in TIME_WAIT.
        port=$(( port + 10 ))

        dotnet "$BENCH" \
          "${transport_args[@]}" \
          --storage "$storage" \
          --wal-dir "$WAL_DIR" \
          --sync-writes "$SYNC_WRITES" \
          --payload-bytes "$PAYLOAD" \
          --batch-size "$batch" \
          --concurrency "$concurrency" \
          --duration "$DURATION" \
          --warmup "$WARMUP" \
          --base-port "$port" \
          --label "$LABEL" \
          --output "$OUTPUT" \
          2>/dev/null | sed -n '/^Kommander.Benchmark$/,/^  Cost/p' || echo "   arm failed (exit $?)"
      done
    done
  done
done

echo
echo "── Fit: round = a + b·n per group"
dotnet "$BENCH" --fit "$OUTPUT" --output "$OUTPUT"
