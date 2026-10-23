#!/usr/bin/env bash
# Runs the TLC model checker on Kommander's TLA+ models.
#
#   formal/run-tlc.sh                       # every config in formal/tla/configs, checked against its EXPECT line
#   formal/run-tlc.sh Fixed                 # one config, verdict checked
#   formal/run-tlc.sh Fixed -workers 1      # extra arguments go to TLC
#   formal/run-tlc.sh --trace <file.tla|dir>... # validate recorded DST node traces (see formal/README.md)
#
# Each config starts with "\* EXPECT: pass", "\* EXPECT: violation <Invariant>" or
# "\* EXPECT: report". A control config that stops producing its counterexample fails the run:
# a model that passes with a known defect switched back on proves nothing.
#
# tla2tools.jar is pinned by version and SHA-256 and downloaded into formal/.tools (gitignored).
# Needs Java 11 or later: JAVA_HOME, then java on PATH, then Homebrew's openjdk.
# TLC_WORKERS sets the worker threads (default 2, so a run does not take every core from other work).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tools="$here/.tools"
tla="$here/tla"

TLA2TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar"
TLA2TOOLS_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

fetch() {
  local url="$1" sha="$2" out="$3"
  if [[ -f "$out" ]] && [[ "$(shasum -a 256 "$out" | cut -d' ' -f1)" == "$sha" ]]; then
    return
  fi
  mkdir -p "$tools"
  echo "Downloading $(basename "$out")..." >&2
  curl -sSfL -o "$out.tmp" "$url"
  local got
  got="$(shasum -a 256 "$out.tmp" | cut -d' ' -f1)"
  if [[ "$got" != "$sha" ]]; then
    rm -f "$out.tmp"
    echo "Checksum mismatch for $url: expected $sha, got $got" >&2
    exit 2
  fi
  mv "$out.tmp" "$out"
}

find_java() {
  if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
    echo "$JAVA_HOME/bin/java"
  elif command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then
    command -v java
  elif [[ -x /opt/homebrew/opt/openjdk@21/bin/java ]]; then
    echo /opt/homebrew/opt/openjdk@21/bin/java
  elif [[ -x /opt/homebrew/opt/openjdk/bin/java ]]; then
    echo /opt/homebrew/opt/openjdk/bin/java
  else
    echo "No Java runtime found. Install Java 11 or later (for example: brew install openjdk@21)." >&2
    exit 2
  fi
}

fetch "$TLA2TOOLS_URL" "$TLA2TOOLS_SHA256" "$tools/tla2tools.jar"
JAVA="$(find_java)"
CP="$tools/tla2tools.jar"

# run_tlc <dir> <spec.tla> <config.cfg> <metadir-name> [tlc args...]; prints TLC output to stdout.
run_tlc() {
  local dir="$1" spec="$2" cfg="$3" name="$4"
  shift 4
  (cd "$dir" && "$JAVA" -XX:+UseParallelGC -Xss64m -cp "$CP" tlc2.TLC \
      -config "$cfg" -metadir "$tools/states/$name" -workers "${TLC_WORKERS:-2}" -cleanup "$@" "$spec")
}

# trace_verdict <trace.tla>: replays one recorded node trace through WalDurabilityTrace and prints
# "pass", "violation <Invariant>" or "error", then a tab and the validated-events summary.
trace_verdict() {
  local trace="$1"
  local name work log
  name="$(basename "$trace" .tla)"
  work="$tools/trace-work/$name"
  log="$tools/logs/trace-$name.log"
  rm -rf "$work"
  mkdir -p "$work" "$tools/logs"
  cp "$tla/WalDurability.tla" "$tla/WalDurabilityTrace.tla" "$tla/WalDurabilityTrace.cfg" "$work/"
  # The validator EXTENDS a module named TraceData; the recorder names each module after its node.
  sed "1s/^---- MODULE .* ----$/---- MODULE TraceData ----/" "$trace" > "$work/TraceData.tla"

  set +e
  TLC_WORKERS=1 run_tlc "$work" WalDurabilityTrace.tla WalDurabilityTrace.cfg "trace-$name" >"$log" 2>&1
  set -e

  local summary violated
  summary="$(sed -n 's/^<<"validated events", \(.*\)>>$/\1/p' "$log" | head -1)"
  violated="$(sed -n 's/^Error: Invariant \(.*\) is violated\.$/\1/p' "$log" | head -1)"
  if grep -q "Model checking completed. No error has been found." "$log"; then
    printf 'pass\t%s' "$summary"
  elif [[ -n "$violated" ]]; then
    printf 'violation %s\t%s' "$violated" "$summary"
  else
    printf 'error\t%s' "$summary"
  fi
}

# check_trace <trace.tla> [expect]: without an expectation, a trace must pass.
check_trace() {
  local trace="$1" expect="${2:-pass}"
  local name result verdict summary
  name="$(basename "$trace" .tla)"
  result="$(trace_verdict "$trace")"
  verdict="${result%%$'\t'*}"
  summary="${result#*$'\t'}"
  if [[ "$verdict" == "$expect" ]]; then
    printf '%-40s %-36s ok   (%s)\n' "$name" "$verdict" "$summary"
    return 0
  fi
  printf '%-40s %-36s EXPECTED %s -- see %s\n' "$name" "$verdict" "$expect" "$tools/logs/trace-$name.log"
  return 1
}

if [[ "${1:-}" == "--trace" ]]; then
  shift
  failed=0
  for target in "$@"; do
    if [[ -d "$target" ]]; then
      for trace in "$target"/*.tla; do
        check_trace "$trace" || failed=1
      done
    else
      check_trace "$target" || failed=1
    fi
  done
  exit $failed
fi

check_config() {
  local name="$1"
  shift
  local cfg="$tla/configs/$name.cfg"
  if [[ ! -f "$cfg" ]]; then
    echo "No config $cfg" >&2
    return 2
  fi
  local expect
  expect="$(sed -n 's/^\\\* EXPECT: //p' "$cfg" | head -1)"
  local log="$tools/logs/$name.log"
  mkdir -p "$tools/logs"

  local started=$SECONDS
  set +e
  run_tlc "$tla" KommanderDurability.tla "configs/$name.cfg" "$name" "$@" >"$log" 2>&1
  set -e
  local elapsed=$((SECONDS - started))

  local verdict
  if grep -q "Model checking completed. No error has been found." "$log"; then
    verdict="pass"
  elif violated="$(sed -n 's/^Error: Invariant \(.*\) is violated\.$/\1/p' "$log" | head -1)" && [[ -n "$violated" ]]; then
    verdict="violation $violated"
  elif grep -q "^Error: Action property .* is violated\|^Error: Temporal properties were violated" "$log"; then
    verdict="violation $(grep -o 'Action property [A-Za-z]*' "$log" | head -1 | cut -d' ' -f3)"
  else
    verdict="error"
  fi

  local states
  states="$(grep -o '[0-9,]* distinct states found' "$log" | tail -1 || true)"
  if [[ "$expect" == "report" ]]; then
    printf '%-20s %-48s (%s s, %s) [report only]\n' "$name" "$verdict" "$elapsed" "${states:-?}"
    return 0
  fi
  if [[ "$verdict" == "$expect" ]]; then
    printf '%-20s %-48s (%s s, %s) ok\n' "$name" "$verdict" "$elapsed" "${states:-?}"
    return 0
  fi
  printf '%-20s %-48s (%s s) EXPECTED %s -- see %s\n' "$name" "$verdict" "$elapsed" "$expect" "$log"
  return 1
}

if [[ $# -ge 1 ]]; then
  name="$1"
  shift
  check_config "$name" "$@"
  exit $?
fi

failed=0
for cfg in "$tla"/configs/*.cfg; do
  check_config "$(basename "$cfg" .cfg)" || failed=1
done
# Stored traces: a passing one, a control the validator must reject, and recorded findings.
for trace in "$tla"/traces/*.tla; do
  check_trace "$trace" "$(sed -n 's/^\\\* EXPECT: //p' "$trace" | head -1)" || failed=1
done
exit $failed
