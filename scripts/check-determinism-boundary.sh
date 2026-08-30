#!/usr/bin/env bash
#
# Determinism boundary guard.
#
# Deterministic Simulation Testing replays a run from a seed. A replay is exact only while
# every elapsed-time gate in the consensus path reads ticks from RaftConfiguration.TickSource.
# One new direct read of the process clock is enough to make a run unreplayable, and the
# failure is silent: the run still passes, it just stops being reproducible.
#
# This script fails the build when a consensus-path file reads the clock directly, outside the
# approved seams listed below. It is a text check on purpose. An analyzer would be stricter,
# but this runs in CI with no toolchain of its own.
#
# Approved seams (the only places allowed to read the process clock):
#   Kommander/Time/SystemMonotonicTickSource.cs   the production tick source itself
#   Kommander/Scheduling/IRaftPartitionHost.cs    the interface default, for test hosts
#   Kommander/Diagnostics/ValueStopwatch.cs       latency measurement for logs and metrics
#
# Usage:
#   scripts/check-determinism-boundary.sh          check the repository
#   scripts/check-determinism-boundary.sh --list   print every match, allowed or not

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Files and directories that carry the Raft consensus path.
CONSENSUS_PATHS=(
  "Kommander/Consensus"
  "Kommander/Scheduling"
  "Kommander/WAL"
  "Kommander/System"
  "Kommander/RaftPartition.cs"
  "Kommander/RaftPartitionStateMachine.cs"
  "Kommander/RaftWriteAhead.cs"
  "Kommander/RaftManager.cs"
  "Kommander/RaftTimerService.cs"
  "Kommander/LoadReportService.cs"
  "Kommander/SnapshotSender.cs"
  "Kommander/SnapshotReceiver.cs"
  "Kommander/ReplicationGateway.cs"
)

# Exact files allowed to read the process clock. One per line, repository-relative.
ALLOWED_FILES=(
  "Kommander/Time/SystemMonotonicTickSource.cs"
  "Kommander/Scheduling/IRaftPartitionHost.cs"
  "Kommander/Diagnostics/ValueStopwatch.cs"
)

# Patterns that leak wall-clock or thread-pool nondeterminism into a simulated run.
# Each entry is "label|extended-regex".
PATTERNS=(
  "wall-clock-ticks|Stopwatch[[:space:]]*\.[[:space:]]*GetTimestamp[[:space:]]*\("
  "wall-clock-date|(DateTime|DateTimeOffset)[[:space:]]*\.[[:space:]]*(UtcNow|Now)"
  "thread-pool-hop|Task[[:space:]]*\.[[:space:]]*Run[[:space:]]*\("
  "wall-clock-sleep|(Task[[:space:]]*\.[[:space:]]*Delay|Thread[[:space:]]*\.[[:space:]]*Sleep)[[:space:]]*\("
)

LIST_ONLY=0
if [[ "${1:-}" == "--list" ]]; then
  LIST_ONLY=1
fi

is_allowed_file() {
  local candidate="$1"
  local allowed
  for allowed in "${ALLOWED_FILES[@]}"; do
    [[ "$candidate" == "$allowed" ]] && return 0
  done
  return 1
}

# Baseline of accepted pre-existing matches, by "file:pattern-label" and count.
# A file may exceed its baseline only after the extra reads are routed through the tick
# source, or after the baseline is raised deliberately in the same change.
BASELINE_FILE="scripts/determinism-boundary-baseline.txt"

collect() {
  local label regex path
  for entry in "${PATTERNS[@]}"; do
    label="${entry%%|*}"
    regex="${entry#*|}"
    for path in "${CONSENSUS_PATHS[@]}"; do
      [[ -e "$path" ]] || continue
      # -r walks a directory; a single file argument works the same way.
      grep -rEIn --include='*.cs' "$regex" "$path" 2>/dev/null | while IFS= read -r hit; do
        local file="${hit%%:*}"
        is_allowed_file "$file" && continue
        printf '%s\t%s\n' "$label" "$hit"
      done
    done
  done
}

MATCHES="$(collect | sort -u || true)"

if [[ "$LIST_ONLY" -eq 1 ]]; then
  printf '%s\n' "$MATCHES"
  exit 0
fi

# Reduce to per-file, per-label counts and compare against the baseline.
CURRENT="$(printf '%s\n' "$MATCHES" \
  | awk -F'\t' 'NF==2 { split($2, parts, ":"); print parts[1] "|" $1 }' \
  | sort | uniq -c | awk '{ print $2 " " $1 }' | sort)"

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "Determinism boundary: no baseline at $BASELINE_FILE."
  echo "Write the current counts there to accept them:"
  printf '%s\n' "$CURRENT"
  exit 1
fi

BASELINE="$(grep -vE '^[[:space:]]*(#|$)' "$BASELINE_FILE" | sort)"

FAILED=0
while read -r key count; do
  [[ -z "${key:-}" ]] && continue
  allowed_count="$(awk -v k="$key" '$1 == k { print $2 }' <<<"$BASELINE")"
  allowed_count="${allowed_count:-0}"
  if (( count > allowed_count )); then
    echo "FAIL  $key: $count occurrences, baseline allows $allowed_count."
    FAILED=1
  fi
done <<<"$CURRENT"

if (( FAILED )); then
  cat <<'EOF'

A consensus-path file gained a direct wall-clock or thread-pool read.
Route it through RaftConfiguration.TickSource (see Kommander/Time/IMonotonicTickSource.cs),
or, if the read is measurement-only and cannot affect a decision, raise the baseline in
scripts/determinism-boundary-baseline.txt in the same change and say why in the commit.

Run 'scripts/check-determinism-boundary.sh --list' to see every match.
EOF
  exit 1
fi

echo "Determinism boundary: OK (no consensus-path file exceeds its baseline)."
