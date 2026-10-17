using System.Reflection;
using BenchmarkDotNet.Running;

// Entry point for the Kommander allocation/CPU micro-benchmarks. Each benchmark suite measures
// the real library code paths (not reimplementations), so "before vs after" allocation numbers
// are measured rather than assumed.
//
// Usage (always Release — BenchmarkDotNet rejects Debug builds):
//   dotnet run -c Release -f net8.0 --project Kommander.MicroBenchmarks                # interactive picker
//   dotnet run -c Release -f net8.0 --project Kommander.MicroBenchmarks -- --filter '*LogOrdering*'
//   dotnet run -c Release -f net8.0 --project Kommander.MicroBenchmarks -- --list flat
//   dotnet run -c Release -f net10.0 --project Kommander.MicroBenchmarks -- handoff [spin-us]  # read-scheduler hand-off report
if (args.Length > 0 && args[0] == "handoff")
{
    // Not a BenchmarkDotNet suite: the cost it measures is CPU on other threads, which BDN does not report.
    Kommander.MicroBenchmarks.ReadSchedulerHandoffReport.Run(args);
    return;
}

BenchmarkSwitcher.FromAssembly(Assembly.GetExecutingAssembly()).Run(args);
