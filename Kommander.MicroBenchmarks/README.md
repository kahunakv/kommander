# Kommander.MicroBenchmarks

Allocation/CPU micro-benchmarks for the Kommander library. Each suite measures the **real library
code path** (via `InternalsVisibleTo`), comparing the pre-change behavior against the current
implementation so "before vs after" numbers are measured, not assumed.

These are micro-benchmarks (single methods, in-process). They are intentionally separate from any
macro throughput tool that measures committed logs/sec across cluster shapes, which is a different
concern.

## Running

BenchmarkDotNet requires an optimized build and refuses to run under `-c Debug`.

```sh
# Interactive picker
dotnet run -c Release -f net8.0 --project Kommander.MicroBenchmarks

# Run one suite
dotnet run -c Release -f net8.0 --project Kommander.MicroBenchmarks -- --filter '*LogOrdering*'
dotnet run -c Release -f net8.0 --project Kommander.MicroBenchmarks -- --filter '*AuthMetadata*'

# List everything
dotnet run -c Release -f net8.0 --project Kommander.MicroBenchmarks -- --list flat
```

Swap `-f net8.0` for `-f net10.0` to measure the .NET 10 build.

## Suites

| Suite | What it shows |
|-------|---------------|
| `LogOrderingBenchmarks` | `OrderById` allocates 0 bytes for already-sorted batches (the common case) vs the old `OrderBy(...).ToArray()`; parity on the unsorted fallback. |
| `AuthMetadataBenchmarks` | Old path signs HMAC + allocates `Metadata` per send; the new warm-send path does neither (signing moves to once-per-stream-open). |
| `LeadershipLeaseBenchmarks` | A published-lease confirmation hit (`TryConfirmLeadershipFast`, and the async hit chain) allocates 0 bytes vs the executor-side fast path's 80 B `RaftResponse` per call (which itself excludes the full `Ask` round trip). |
| `ReadSchedulerBenchmarks` | Bytes/read through the real `FairReadScheduler`: state-carried `EnqueueTask<TState,T>` (item + `Task` only) vs the legacy `Func<T>` overload vs the pre-change TCS + closure + `Action` shape. |
| `WalEnqueueBenchmarks` | Submit-to-durable cost through the real `FairWalScheduler.Enqueue`, warm partition vs first admission. The one suite that calls the production write-scheduler entry point; `WalSchedulerListBenchmarks` only models the list allocation inside the flush loop. |
| `AuthValidationBenchmarks` | Receiver-side shared-secret `Validate` with a distinct nonce per iteration, accepted and rejected, across body sizes. Complements `AuthMetadataBenchmarks`, which measures the sending side only. |

## Adding a suite

1. Add a `[MemoryDiagnoser]` class with `[Benchmark]` methods; mark the pre-change variant
   `[Benchmark(Baseline = true)]`.
2. Benchmark real code. If a symbol is `internal`, this assembly already has access; if it is
   `private`, prefer reproducing the exact pre-change body (clearly commented) over loosening
   production visibility — only widen visibility when measuring the *new* code path requires it.
3. Prefer the production entry point over an expression that resembles it. A benchmark that
   reconstructs a code path can drift from the wiring it claims to measure, and a suite that never
   calls the real method cannot show what a change to that method did.
4. Say what the number does **not** include. Work on a scheduler's worker threads is not attributable
   to the submitting thread, so an allocation figure for a submit path is a floor, not a total.
