using System.Diagnostics;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

// Smoke check for the thread-free embedded mode. It boots a single-node Kommander with the
// in-memory write-ahead log, waits for the node to elect itself, commits a few proposals, and tears
// the node down. It prints one line per phase, so a failure names the phase that failed.
//
// Exit codes: 0 pass, 1 a check failed, 2 a phase did not finish in time (a deadlock or a stall).
//
// Nothing here may block the thread. The single-threaded runtime has no other thread to release a
// blocked wait, so every wait is an await, and the deadline is a Task.WhenAny race.

const int Proposals = 5;
TimeSpan phaseTimeout = TimeSpan.FromSeconds(30);

Stopwatch total = Stopwatch.StartNew();
ILogger<IRaft> logger = new ConsoleLogger();

Console.WriteLine($"[smoke] runtime: browser={OperatingSystem.IsBrowser()} framework={Environment.Version}");

RaftConfiguration config = new()
{
    NodeName = "node1",
    NodeId = 1,
    Host = "localhost",
    Port = 8001,
    InitialPartitions = 1,
    HeartbeatInterval = TimeSpan.FromMilliseconds(50),
    RecentHeartbeat = TimeSpan.FromMilliseconds(25),
    VotingTimeout = TimeSpan.FromMilliseconds(250),
    CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
    UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
    TimerInitialDelay = TimeSpan.FromMilliseconds(25),
    StartElectionTimeout = 100,
    EndElectionTimeout = 250,
    EnableQuiescence = false,
};

try
{
    // The browser build refuses certificate-based transport security in Validate. The certificate
    // code of the REST client is suppressed for the platform analyzer on the strength of this check,
    // so a regression here would let that code fail at run time instead.
    try
    {
        new RaftConfiguration
        {
            TransportSecurity = new RaftTransportSecurityOptions { NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls },
        }.Validate();

        Check(false, "Validate refuses MutualTls in the browser build");
    }
    catch (RaftException ex) when (ex.Message.Contains("browser build"))
    {
        Check(true, "Validate refuses MutualTls in the browser build");
    }

    RaftManager node = new(
        config,
        new StaticDiscovery([]),
        new InMemoryWAL(logger),
        new InMemoryCommunication(),
        new HybridLogicalClock(),
        logger);

    Console.WriteLine($"[smoke] constructed at {total.ElapsedMilliseconds} ms");

    using CancellationTokenSource cts = new(TimeSpan.FromSeconds(60));

    await Phase("join", node.JoinCluster(cts.Token));
    Check(node.IsInitialized, "node is initialized after JoinCluster");

    await Phase("elect", node.WaitForLeader(1, cts.Token).AsTask());
    Check(await node.AmILeader(1, cts.Token), "node leads partition 1");

    long before = node.WalAdapter.GetMaxLog(1);

    for (int i = 0; i < Proposals; i++)
    {
        RaftReplicationResult result = await PhaseOf(
            $"propose {i + 1}/{Proposals}",
            node.ReplicateLogs(1, "Smoke", global::System.Text.Encoding.UTF8.GetBytes($"entry-{i}"), cancellationToken: cts.Token));

        Check(result.Success, $"proposal {i + 1} succeeded (status {result.Status})");
    }

    long after = node.WalAdapter.GetMaxLog(1);
    Check(after - before >= Proposals, $"write-ahead log advanced by at least {Proposals} (from {before} to {after})");

    await Phase("leave", node.LeaveCluster(true, CancellationToken.None));

    Console.WriteLine($"[smoke] PASS in {total.ElapsedMilliseconds} ms");
    return 0;
}
catch (SmokeCheckFailed ex)
{
    Console.WriteLine($"[smoke] FAIL: {ex.Message}");
    return 1;
}
catch (SmokePhaseTimedOut ex)
{
    Console.WriteLine($"[smoke] FAIL: {ex.Message}");
    return 2;
}
catch (Exception ex)
{
    Console.WriteLine($"[smoke] FAIL: unexpected {ex}");
    return 1;
}

async Task Phase(string name, Task task)
{
    Stopwatch watch = Stopwatch.StartNew();

    if (await Task.WhenAny(task, Task.Delay(phaseTimeout)) != task)
        throw new SmokePhaseTimedOut($"phase '{name}' did not finish in {phaseTimeout.TotalSeconds:F0} s");

    await task;
    Console.WriteLine($"[smoke] {name}: ok in {watch.ElapsedMilliseconds} ms (at {total.ElapsedMilliseconds} ms)");
}

async Task<T> PhaseOf<T>(string name, Task<T> task)
{
    await Phase(name, (Task)task);
    return await task;
}

static void Check(bool condition, string what)
{
    if (!condition)
        throw new SmokeCheckFailed(what);

    Console.WriteLine($"[smoke] check: {what}");
}

sealed class SmokeCheckFailed(string message) : Exception(message);

sealed class SmokePhaseTimedOut(string message) : Exception(message);

/// <summary>
/// Writes warnings and errors to the console. Microsoft.Extensions.Logging.Console is not used: its
/// processor writes from a dedicated thread, which the single-threaded runtime cannot start.
/// </summary>
sealed class ConsoleLogger : ILogger<IRaft>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel))
            return;

        Console.WriteLine($"[kommander:{logLevel}] {formatter(state, exception)}{(exception is null ? "" : " " + exception)}");
    }
}
