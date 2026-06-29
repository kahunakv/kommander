using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Runs every benchmark with the in-process (emit) toolchain instead of BenchmarkDotNet's default
/// "generate + build + spawn a child exe" toolchain.
/// <para>
/// Why this is required: the default toolchain rebuilds the referenced <c>Kommander</c> project into
/// a per-run redirected output path. <c>Kommander</c> uses <c>Grpc.Tools</c> protoc codegen, which
/// does not survive the redirected <c>IntermediateOutputPath</c> — protoc reports "expected outputs
/// were not generated" and the boilerplate build fails with <c>CS2001</c> on the generated
/// <c>*Grpc.cs</c> files. Running in-process reuses the already-built <c>Kommander.dll</c>, so
/// codegen never re-runs.
/// </para>
/// <para>
/// In-process execution is slightly less isolated than a dedicated process, but for these pure
/// CPU/allocation micro-benchmarks (no native interop, no separate-process state) it is accurate and
/// is the toolchain BenchmarkDotNet recommends when the dependency graph cannot be rebuilt cleanly.
/// </para>
/// </summary>
public sealed class InProcessConfig : ManualConfig
{
    public InProcessConfig()
    {
        AddJob(Job.Default.WithToolchain(InProcessEmitToolchain.Instance));
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}
