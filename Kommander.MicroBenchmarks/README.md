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

## Adding a suite

1. Add a `[MemoryDiagnoser]` class with `[Benchmark]` methods; mark the pre-change variant
   `[Benchmark(Baseline = true)]`.
2. Benchmark real code. If a symbol is `internal`, this assembly already has access; if it is
   `private`, prefer reproducing the exact pre-change body (clearly commented) over loosening
   production visibility — only widen visibility when measuring the *new* code path requires it.
