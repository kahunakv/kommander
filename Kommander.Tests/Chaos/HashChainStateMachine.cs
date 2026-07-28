
using System.Buffers.Binary;
using System.Text;
using Kommander;
using Kommander.Data;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Test-only observer that records the exact history of committed user log entries a single node applied
/// on one partition, as a hash chain, so cross-node divergence can be detected precisely.
///
/// <para>One instance is created per (node, user partition) and subscribed to
/// <see cref="RaftManager.OnReplicationReceived"/>. For every applied entry it folds a canonical,
/// length-delimited, little-endian encoding of the entry into a rolling prefix hash and stores the prefix
/// digest by log index. Two nodes that applied the identical prefix therefore have identical digests at
/// every shared index; the first index at which their digests differ localises a state-machine
/// divergence (see <see cref="HashChainAssert"/>).</para>
///
/// <para><b>Duplicate delivery</b> is detected via a per-entry content digest (independent of the chain):
/// re-delivering the same index with identical content is an apply-idempotency violation; re-delivering
/// it with different content is immediate divergence. Neither is folded into the chain twice. A delivery
/// whose index is not strictly greater than the last applied index (and is not a known duplicate) is an
/// ordering violation. All state is guarded by a per-instance lock so it is safe to subscribe on the
/// executor callback path.</para>
///
/// <para><b>Not</b> based on <c>string.GetHashCode</c>, process-randomized hashing, or native-endian
/// serialization: the encoding fixes little-endian byte order and length-delimits every variable field,
/// and the digest is the repository xxHash primitive (<see cref="HashUtils.HashBytes"/>), so a recorded
/// history is stable and comparable across processes.</para>
/// </summary>
public sealed class HashChainStateMachine
{
    private const ulong Seed = 0UL;

    private readonly object _lock = new();
    private readonly Dictionary<long, ulong> _prefixHashByIndex = new();
    private readonly Dictionary<long, ulong> _entryDigestByIndex = new();
    private readonly Dictionary<long, EntryMeta> _metaByIndex = new();

    private readonly List<long> _idempotencyViolations = [];
    private readonly List<ConflictingDuplicate> _conflictingDuplicates = [];
    private readonly List<OrderingViolation> _orderingViolations = [];

    private ulong _currentHash = Seed;
    private long _lastAppliedIndex = -1;
    private long _appliedCount;

    public HashChainStateMachine(string endpoint, int partitionId)
    {
        Endpoint = endpoint;
        PartitionId = partitionId;
    }

    public string Endpoint { get; }
    public int PartitionId { get; }

    /// <summary>
    /// Subscribe this to <see cref="RaftManager.OnReplicationReceived"/>. Ignores entries for other
    /// partitions (the event is per-node, not per-partition) and always acks. Return value is not relied
    /// upon by callers because <see cref="RaftManager.OnReplicationReceived"/> is a multicast delegate.
    /// </summary>
    public Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
    {
        if (partitionId != PartitionId)
            return Task.FromResult(true);

        lock (_lock)
        {
            long index = log.Id;
            ulong entryDigest = ComputeEntryDigest(log);
            int payloadLength = log.LogData?.Length ?? 0;
            EntryMeta meta = new(index, log.Term, log.Type, log.LogType, payloadLength, entryDigest);

            if (_prefixHashByIndex.ContainsKey(index))
            {
                // Duplicate delivery of an index we already folded.
                if (_entryDigestByIndex[index] == entryDigest)
                    _idempotencyViolations.Add(index);               // identical duplicate
                else
                    _conflictingDuplicates.Add(new ConflictingDuplicate(index, _metaByIndex[index], meta)); // divergence
                return Task.FromResult(true);                        // never fold a duplicate twice
            }

            if (index <= _lastAppliedIndex)
            {
                // Out-of-order / reordered delivery: an index we have not seen but that is not strictly
                // greater than the last applied one. Recorded, not folded (folding would corrupt the chain).
                _orderingViolations.Add(new OrderingViolation(index, _lastAppliedIndex));
                return Task.FromResult(true);
            }

            // Strictly increasing index (a jump > lastApplied+1 leaves a hole detected at assert time).
            ulong chain = ChainHash(_currentHash, entryDigest);
            _prefixHashByIndex[index] = chain;
            _entryDigestByIndex[index] = entryDigest;
            _metaByIndex[index] = meta;
            _currentHash = chain;
            _lastAppliedIndex = index;
            _appliedCount++;
            return Task.FromResult(true);
        }
    }

    /// <summary>Returns an immutable point-in-time copy of this node's applied history for comparison.</summary>
    public HashChainSnapshot Snapshot()
    {
        lock (_lock)
        {
            return new HashChainSnapshot(
                Endpoint,
                PartitionId,
                _lastAppliedIndex,
                _appliedCount,
                _currentHash,
                new Dictionary<long, ulong>(_prefixHashByIndex),
                new Dictionary<long, EntryMeta>(_metaByIndex),
                [.. _idempotencyViolations],
                [.. _conflictingDuplicates],
                [.. _orderingViolations]);
        }
    }

    // ── canonical encoding ──────────────────────────────────────────────────────

    /// <summary>
    /// Digests the entry content (index, term, Raft entry type, application log type, payload) with a
    /// canonical little-endian, length-delimited encoding. Independent of history, so it identifies
    /// whether two deliveries of the same index carry identical content.
    /// </summary>
    private static ulong ComputeEntryDigest(RaftLog log)
    {
        ReadOnlySpan<byte> logTypeBytes = log.LogType is null
            ? default
            : Encoding.UTF8.GetBytes(log.LogType);
        ReadOnlySpan<byte> payload = log.LogData is null ? default : log.LogData;

        int size = 8 /*Id*/ + 8 /*Term*/ + 4 /*Type*/ + 4 /*logTypeLen*/ + logTypeBytes.Length
                   + 4 /*payloadLen*/ + payload.Length;
        byte[] buffer = new byte[size];
        Span<byte> span = buffer;
        int o = 0;
        BinaryPrimitives.WriteInt64LittleEndian(span[o..], log.Id); o += 8;
        BinaryPrimitives.WriteInt64LittleEndian(span[o..], log.Term); o += 8;
        BinaryPrimitives.WriteInt32LittleEndian(span[o..], (int)log.Type); o += 4;
        BinaryPrimitives.WriteInt32LittleEndian(span[o..], logTypeBytes.Length); o += 4;
        logTypeBytes.CopyTo(span[o..]); o += logTypeBytes.Length;
        BinaryPrimitives.WriteInt32LittleEndian(span[o..], payload.Length); o += 4;
        payload.CopyTo(span[o..]);

        return HashUtils.HashBytes(buffer);
    }

    /// <summary>Folds an entry digest into the rolling prefix hash: chain = H(previousChain || entryDigest).</summary>
    private static ulong ChainHash(ulong previousChain, ulong entryDigest)
    {
        Span<byte> b = stackalloc byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(b, previousChain);
        BinaryPrimitives.WriteUInt64LittleEndian(b[8..], entryDigest);
        return HashUtils.HashBytes(b);
    }
}

/// <summary>Immutable metadata for one applied entry, retained for mismatch reporting.</summary>
public sealed record EntryMeta(long Index, long Term, RaftLogType Type, string? LogType, int PayloadLength, ulong EntryDigest)
{
    public override string ToString() =>
        $"idx={Index} term={Term} type={Type} logType={LogType ?? "<null>"} payloadLen={PayloadLength} digest={EntryDigest:X16}";
}

/// <summary>A duplicate delivery of the same index whose content differs from the first — a divergence.</summary>
public sealed record ConflictingDuplicate(long Index, EntryMeta First, EntryMeta Second);

/// <summary>A delivery whose index was not strictly greater than the last applied index.</summary>
public sealed record OrderingViolation(long Index, long LastAppliedIndex);

/// <summary>Immutable snapshot of one node's applied history on one partition.</summary>
public sealed class HashChainSnapshot
{
    public HashChainSnapshot(
        string endpoint,
        int partitionId,
        long lastAppliedIndex,
        long appliedCount,
        ulong currentHash,
        IReadOnlyDictionary<long, ulong> prefixHashByIndex,
        IReadOnlyDictionary<long, EntryMeta> metaByIndex,
        IReadOnlyList<long> idempotencyViolations,
        IReadOnlyList<ConflictingDuplicate> conflictingDuplicates,
        IReadOnlyList<OrderingViolation> orderingViolations)
    {
        Endpoint = endpoint;
        PartitionId = partitionId;
        LastAppliedIndex = lastAppliedIndex;
        AppliedCount = appliedCount;
        CurrentHash = currentHash;
        PrefixHashByIndex = prefixHashByIndex;
        MetaByIndex = metaByIndex;
        IdempotencyViolations = idempotencyViolations;
        ConflictingDuplicates = conflictingDuplicates;
        OrderingViolations = orderingViolations;
    }

    public string Endpoint { get; }
    public int PartitionId { get; }
    public long LastAppliedIndex { get; }

    /// <summary>Number of entries folded. NOT a valid comparison coordinate on its own: snapshots and
    /// non-user entries can make this differ from the Raft index. Compare by index, not count.</summary>
    public long AppliedCount { get; }

    public ulong CurrentHash { get; }
    public IReadOnlyDictionary<long, ulong> PrefixHashByIndex { get; }
    public IReadOnlyDictionary<long, EntryMeta> MetaByIndex { get; }
    public IReadOnlyList<long> IdempotencyViolations { get; }
    public IReadOnlyList<ConflictingDuplicate> ConflictingDuplicates { get; }
    public IReadOnlyList<OrderingViolation> OrderingViolations { get; }

    public bool HasDuplicateApply =>
        IdempotencyViolations.Count > 0 || ConflictingDuplicates.Count > 0;
}
