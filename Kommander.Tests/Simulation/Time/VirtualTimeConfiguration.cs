namespace Kommander.Tests.Simulation.Time;

/// <summary>
/// Timer intervals used by the virtual clock, aligned with production Raft defaults.
/// </summary>
public sealed record VirtualTimeConfiguration
{
    public long TimerInitialDelayMs { get; init; } = 2500;
    public long CheckLeaderIntervalMs { get; init; } = 250;
    public long HeartbeatIntervalMs { get; init; } = 500;
    public long UpdateNodesIntervalMs { get; init; } = 5000;
    public int StartElectionTimeoutMs { get; init; } = 2000;
    public int EndElectionTimeoutMs { get; init; } = 4000;
    public long ProposalRetryDelayMs { get; init; } = 10;
    public long ProposalPollDelayMs { get; init; } = 10;

    public static VirtualTimeConfiguration Default { get; } = new();
}
