namespace Kommander.Tests.Simulation;

/// <summary>
/// Categories of events that the deterministic scheduler may select during a simulation run.
/// </summary>
public enum SimulationEventType
{
    TimerTick = 0,
    NetworkMessageDelivery = 1,
    NetworkMessageDrop = 2,
    WalReadCompletion = 3,
    WalWriteCompletion = 4,
    WalWriteFailure = 5,
    ClientProposalStart = 6,
    ClientProposalCancellation = 7,
    NodeStart = 8,
    NodeStop = 9,
    NodeCrash = 10,
    NodeRestart = 11,
    PartitionConfigurationUpdate = 12,
    CompactionStep = 13,
}
