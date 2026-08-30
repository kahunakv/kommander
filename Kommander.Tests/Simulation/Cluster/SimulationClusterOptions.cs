namespace Kommander.Tests.Simulation.Cluster;

/// <summary>
/// Parameters of a simulated cluster. Every value is recorded in the replay log, so a run is
/// reproducible from the options plus the seed.
/// </summary>
public sealed record SimulationClusterOptions
{
    /// <summary>Number of nodes. Raft needs an odd count for a clean majority.</summary>
    public int NodeCount { get; init; } = 3;

    /// <summary>Number of user partitions each node starts with.</summary>
    public int PartitionCount { get; init; } = 1;

    /// <summary>
    /// Seed for the run. It feeds <see cref="RaftConfiguration.ElectionTimeoutSeed"/> on each
    /// node, so the randomized election timeout is a function of the seed and the node index
    /// rather than of the process clock.
    /// </summary>
    public ulong Seed { get; init; } = 1;

    /// <summary>First TCP port. Node <c>i</c> listens on <c>BasePort + i</c>.</summary>
    public int BasePort { get; init; } = 8001;

    /// <summary>Election timeout lower bound in milliseconds, in simulated time.</summary>
    public int StartElectionTimeoutMs { get; init; } = 100;

    /// <summary>Election timeout upper bound in milliseconds, in simulated time.</summary>
    public int EndElectionTimeoutMs { get; init; } = 250;

    /// <summary>Heartbeat cadence in milliseconds, in simulated time.</summary>
    public int HeartbeatIntervalMs { get; init; } = 50;

    /// <summary>
    /// Cadence of the membership refresh in milliseconds, in simulated time. The harness drives
    /// it on this cadence because the internal timer that normally would is switched off.
    /// </summary>
    public long UpdateNodesIntervalMs { get; init; } = 100;

    /// <summary>
    /// Applied to every node's configuration after the defaults above. Use it to sample the
    /// safety-relevant configuration surface per run.
    /// </summary>
    public Action<RaftConfiguration>? ConfigureNode { get; init; }

    /// <summary>Scenario parameters for the replay-log header.</summary>
    public IReadOnlyDictionary<string, string> ToParameters() =>
        new Dictionary<string, string>
        {
            ["nodeCount"] = NodeCount.ToString(),
            ["partitionCount"] = PartitionCount.ToString(),
            ["seed"] = Seed.ToString(),
            ["startElectionTimeoutMs"] = StartElectionTimeoutMs.ToString(),
            ["endElectionTimeoutMs"] = EndElectionTimeoutMs.ToString(),
            ["heartbeatIntervalMs"] = HeartbeatIntervalMs.ToString(),
        };
}
