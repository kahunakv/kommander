
using System.Diagnostics;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Acceptance test for the whole-partition seeding contract
/// (<see cref="IRaftPartitionStateTransfer"/>): a follower below the leader's compaction floor is
/// seeded from a whole-partition export that is <b>newer</b> than the snapshot index — the shape an
/// MVCC application produces, since it snapshots by timestamp and cannot cut an exact as-of-index
/// export — and the WAL suffix retained above the boundary is replayed onto the imported state
/// without regressing it.
///
/// <para>Runs against the REAL WAL layer (<see cref="RaftWriteAhead"/> over
/// <see cref="InMemoryWAL"/> with real schedulers) so the boundary install's term-matched suffix
/// retention, the commit-frontier seed, and the post-install drain are the production code paths,
/// not stubs. The follower's WAL holds live-shipped entries 5..7 above a backfill gap (3..4) —
/// exactly what a lagging replica holds when the leader's floor overtakes its backfill — and the
/// snapshot arrives at boundary index 5 with an import reflecting index 6.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestPartitionStateSeeding
{
    /// <summary>
    /// Minimal revision-monotonic application store. Its apply is idempotent — re-applying an
    /// entry already reflected in an imported snapshot is a no-op — which is precisely the
    /// obligation the seeding contract places on applications. Records every observed value so
    /// the test can assert state never regressed.
    /// </summary>
    private sealed class RevisionedStore : IRaftPartitionStateTransfer
    {
        public long Value { get; private set; }
        public int ImportCount { get; private set; }
        public int ImportedPartitionId { get; private set; } = -1;
        public List<long> AppliedIds { get; } = [];
        public List<long> ValueHistory { get; } = [];

        /// <summary>The revision the exported blob reflects; the import installs it wholesale.</summary>
        public long SnapshotRevision { get; init; }

        public Task<Stream> ExportPartitionState(int partitionId, long upToIndex, CancellationToken ct) =>
            Task.FromResult<Stream>(new MemoryStream([1]));

        public Task ImportPartitionState(int partitionId, Stream snapshot, CancellationToken ct)
        {
            ImportCount++;
            ImportedPartitionId = partitionId;
            Value = Math.Max(Value, SnapshotRevision);
            ValueHistory.Add(Value);
            return Task.CompletedTask;
        }

        public void Apply(long entryId)
        {
            AppliedIds.Add(entryId);
            Value = Math.Max(Value, entryId); // idempotent: already-reflected entries are no-ops
            ValueHistory.Add(Value);
        }
    }

    private sealed class SeedingHost : IRaftPartitionHost
    {
        private readonly RevisionedStore store;

        public SeedingHost(RevisionedStore store) => this.store = store;

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "follower:9000";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;

        public RaftConfiguration Configuration { get; } = new()
        {
            NodeId = 1,
            Host = "follower",
            Port = 9000,
            InitialPartitions = 1,
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
        };

        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [];

        public HLCTimestamp GetLastNodeActivity(string e, int p) => HybridLogicalClock.TrySendOrLocalEvent(1);
        // A fresh heartbeat timestamp keeps the follower from campaigning during the drain tick,
        // so the test observes the pure seed-then-replay sequence.
        public HLCTimestamp GetLastNodeHearthbeat(string e, int p) => HybridLogicalClock.TrySendOrLocalEvent(1);
        public void UpdateLastHeartbeat(string e, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string e, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string e, RaftResponderRequest r) { }
        public Task InvokeLeaderChanged(int p, string l) => Task.CompletedTask;

        public Task<bool> InvokeReplicationReceived(int p, RaftLog log)
        {
            store.Apply(log.Id);
            return Task.FromResult(true);
        }

        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog l) => Task.FromResult(true);
        public void InvokeReplicationError(int p, RaftLog l) { }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public IRaftPartitionStateTransfer? PartitionStateTransfer => store;

        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode n, SnapshotRequest r, CancellationToken ct) =>
            Task.FromResult(new SnapshotResponse(false));

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
    }

    private sealed class NoopSink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }

    [Fact]
    public async Task BelowFloorFollower_SeededFromNewerThanIndexExport_ReplaysRetainedSuffixWithoutRegression()
    {
        const int partitionId = 1;

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9024,
            InitialPartitions = 0,
        };

        using InMemoryWAL wal = new(NullLogger<IRaft>.Instance);

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        RaftPartition partition = new(
            manager,
            wal,
            partitionId,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        RaftWriteAhead writeAhead = new(manager, _ => { }, partition, wal);

        // The exported blob reflects index 6 — newer than the snapshot boundary (5), the shape an
        // MVCC store produces — but below the retained suffix end (7), so the replay must both
        // no-op the already-reflected entry (6) and advance state with the fresh one (7).
        RevisionedStore store = new() { SnapshotRevision = 6 };
        SeedingHost host = new(store);
        RaftPartitionStateMachine sm = new(host, new RaftWalFacadeAdapter(writeAhead), new NoopSink(), NullLogger<IRaft>.Instance);
        sm.SetPostToExecutor(_ => { });

        try
        {
            // The lagging replica's WAL: committed prefix 1..2, a backfill gap at 3..4, and
            // live-shipped committed entries 5..7 (delivered while backfill was pending —
            // the leader's floor then overtook the gap, making backfill impossible).
            writeAhead.EnqueueProposeOrCommit([
                new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t", LogData = [1] },
                new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t", LogData = [1] },
            ]);
            writeAhead.EnqueueProposeOrCommit([
                new RaftLog { Id = 5, Term = 1, Type = RaftLogType.Committed, LogType = "t", LogData = [1] },
                new RaftLog { Id = 6, Term = 1, Type = RaftLogType.Committed, LogType = "t", LogData = [1] },
                new RaftLog { Id = 7, Term = 1, Type = RaftLogType.Committed, LogType = "t", LogData = [1] },
            ]);

            // The gap stalls the commit frontier at 2 — nothing above it can be delivered.
            Assert.Equal(2, writeAhead.GetCommitIndex());

            // Wait for the physical appends to land: the boundary install below term-matches
            // against the STORED row at the boundary index.
            TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(10));
            long started = Stopwatch.GetTimestamp();
            while ((await writeAhead.GetRangeAsync(5, 10)).Count < 3)
            {
                if (Stopwatch.GetElapsedTime(started) > budget)
                    Assert.Fail("physical appends never landed");
                await Task.Delay(5, TestContext.Current.CancellationToken);
            }

            // The snapshot arrives: boundary index 5, matching term, import newer than the index.
            RaftResponse resp = await sm.InstallSnapshotAsync(new SnapshotInstallRequest
            {
                PartitionId = partitionId,
                SnapshotIndex = 5,
                LastIncludedTerm = 1,
                LeaderTerm = 1,
                LeaderEndpoint = "leader:9001",
                Kind = SnapshotKind.PartitionState,
                Snapshot = new MemoryStream([1]),
            });

            Assert.Equal(RaftOperationStatus.Success, resp.Status);
            Assert.Equal(1, store.ImportCount);
            Assert.Equal(partitionId, store.ImportedPartitionId);

            // Term-matched retention: the suffix above the boundary survived the install, and the
            // frontier seed absorbed it — 6..7 are now contiguous committed state.
            Assert.Equal(2, (await writeAhead.GetRangeAsync(6, 10)).Count);
            Assert.Equal(7, writeAhead.GetCommitIndex());

            // The next follower tick replays the retained suffix onto the imported state.
            await sm.CheckPartitionLeadershipAsync();

            // Only the suffix above the boundary is delivered (the import covers 1..5); entry 6
            // was already reflected in the snapshot (idempotent no-op), entry 7 advances state.
            Assert.Equal([6, 7], store.AppliedIds);
            Assert.Equal(7, store.Value);

            // The state never regressed at any observation point.
            long previous = 0;
            foreach (long observed in store.ValueHistory)
            {
                Assert.True(observed >= previous, $"state regressed: {previous} -> {observed}");
                previous = observed;
            }
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();
        }
    }
}
