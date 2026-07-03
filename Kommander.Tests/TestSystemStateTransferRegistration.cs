
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests;

/// <summary>
/// Verifies that <see cref="IRaft.RegisterSystemStateTransfer"/> and its sibling
/// <see cref="IRaft.RegisterStateMachineTransfer"/> are independently readable, replaceable,
/// and clearable on <see cref="RaftManager"/>.
/// </summary>
public class TestSystemStateTransferRegistration
{
    private static RaftManager BuildManager() => new(
        new RaftConfiguration { Host = "localhost", Port = 19900, InitialPartitions = 1 },
        new StaticDiscovery([]),
        new InMemoryWAL(NullLogger<IRaft>.Instance),
        new InMemoryCommunication(),
        new HybridLogicalClock(),
        NullLogger<IRaft>.Instance);

    // ── RegisterSystemStateTransfer ──────────────────────────────────────────

    [Fact]
    public void RegisterSystemStateTransfer_Register_IsReadBack()
    {
        RaftManager manager = BuildManager();
        FakeSystemTransfer transfer = new();

        manager.RegisterSystemStateTransfer(transfer);

        Assert.Same(transfer, manager.SystemStateTransfer);
    }

    [Fact]
    public void RegisterSystemStateTransfer_Replace_ReturnsNewInstance()
    {
        RaftManager manager = BuildManager();
        FakeSystemTransfer first = new();
        FakeSystemTransfer second = new();

        manager.RegisterSystemStateTransfer(first);
        manager.RegisterSystemStateTransfer(second);

        Assert.Same(second, manager.SystemStateTransfer);
    }

    [Fact]
    public void RegisterSystemStateTransfer_ClearToNull_ReturnsNull()
    {
        RaftManager manager = BuildManager();
        manager.RegisterSystemStateTransfer(new FakeSystemTransfer());

        manager.RegisterSystemStateTransfer(null);

        Assert.Null(manager.SystemStateTransfer);
    }

    // ── RegisterStateMachineTransfer (mirror) ─────────────────────────────────

    [Fact]
    public void RegisterStateMachineTransfer_Register_IsReadBack()
    {
        RaftManager manager = BuildManager();
        FakeRangeTransfer transfer = new();

        manager.RegisterStateMachineTransfer(transfer);

        Assert.Same(transfer, manager.StateMachineTransfer);
    }

    [Fact]
    public void RegisterStateMachineTransfer_ClearToNull_ReturnsNull()
    {
        RaftManager manager = BuildManager();
        manager.RegisterStateMachineTransfer(new FakeRangeTransfer());

        manager.RegisterStateMachineTransfer(null);

        Assert.Null(manager.StateMachineTransfer);
    }

    // ── Independence ──────────────────────────────────────────────────────────

    [Fact]
    public void BothTransfers_AreIndependent()
    {
        RaftManager manager = BuildManager();
        FakeSystemTransfer system = new();
        FakeRangeTransfer range = new();

        manager.RegisterSystemStateTransfer(system);
        manager.RegisterStateMachineTransfer(range);

        Assert.Same(system, manager.SystemStateTransfer);
        Assert.Same(range, manager.StateMachineTransfer);

        manager.RegisterSystemStateTransfer(null);

        Assert.Null(manager.SystemStateTransfer);
        Assert.Same(range, manager.StateMachineTransfer);
    }

    // ── Fakes ─────────────────────────────────────────────────────────────────

    private sealed class FakeSystemTransfer : IRaftSystemStateTransfer
    {
        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) => Task.FromResult(Stream.Null);
        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class FakeRangeTransfer : IRaftStateMachineTransfer
    {
        public Task<Stream> ExportRange(RaftSplitPlan plan, long upToIndex, CancellationToken ct) =>
            Task.FromResult(Stream.Null);

        public Task ImportRange(int partitionId, Stream data, CancellationToken ct) =>
            Task.CompletedTask;
    }
}
