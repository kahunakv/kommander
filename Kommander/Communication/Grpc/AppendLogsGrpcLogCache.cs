using Google.Protobuf;
using Google.Protobuf.Collections;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Cached protobuf log entries for one Raft batch fanned out to multiple followers.
/// Built once per replication round; gRPC copies references into each outbound request.
/// </summary>
/// <remarks>
/// <para>
/// <b>Ownership contract.</b> The cached messages wrap each <see cref="RaftLog.LogData"/> array
/// zero-copy via <see cref="UnsafeByteOperations.UnsafeWrap"/> — they alias the caller's arrays
/// rather than copying them. The caller therefore hands ownership of those arrays to this cache for
/// its lifetime, and must treat them as frozen:
/// </para>
/// <list type="bullet">
/// <item>do not mutate a <c>LogData</c> array after passing its <see cref="RaftLog"/> here;</item>
/// <item>do not return one to an <see cref="System.Buffers.ArrayPool{T}"/>, where a later renter
/// would overwrite it.</item>
/// </list>
/// <para>
/// Violating that does not fail loudly. The cache outlives a single send — that is its whole
/// purpose, since one built batch is fanned out to every follower and re-sent on retry — so a
/// mutation between sends means two followers silently receive <em>different bytes</em> under the
/// same log id and term. There is no checksum on the append path to catch it, so the first symptom
/// would be a replica divergence far from the cause.
/// </para>
/// <para>
/// No runtime guard is installed deliberately. A defensive copy would remove the allocation saving
/// this type exists for, and verifying a digest per entry per follower would put hashing on the
/// replication fan-out path. The contract is enforced by review, and by the fact that every current
/// producer of these arrays allocates them fresh per batch.
/// </para>
/// </remarks>
public sealed class AppendLogsGrpcLogCache
{
    private readonly object _lock = new();

    // volatile: the lock-free fast-path read in GetOrCreate is classic double-checked locking —
    // without the barrier a reader on a weak memory model could observe the reference before the
    // list's contents are fully published. Free on x64; required for correctness elsewhere.
    private volatile RepeatedField<GrpcRaftLog>? _logs;

    /// <summary>For tests: how many times the protobuf log list was materialized.</summary>
    internal int BuildCount { get; private set; }

    /// <summary>
    /// Returns the cached <see cref="GrpcRaftLog"/> list for <paramref name="source"/>,
    /// building it on first use. Thread-safe for concurrent follower sends.
    /// </summary>
    /// <param name="source">
    /// Log entries for this batch. Their <see cref="RaftLog.LogData"/> arrays are aliased, not
    /// copied, and must not be mutated or pool-returned afterwards — see the type-level remarks.
    /// </param>
    internal RepeatedField<GrpcRaftLog> GetOrCreate(IReadOnlyList<RaftLog> source)
    {
        if (_logs is not null)
            return _logs;

        lock (_lock)
        {
            if (_logs is not null)
                return _logs;

            RepeatedField<GrpcRaftLog> logs = [];

            foreach (RaftLog requestLog in source)
            {
                logs.Add(new GrpcRaftLog
                {
                    Id = requestLog.Id,
                    Term = requestLog.Term,
                    Type = (GrpcRaftLogType)requestLog.Type,
                    LogType = requestLog.LogType,
                    TimeNode = requestLog.Time.N,
                    TimePhysical = requestLog.Time.L,
                    TimeCounter = requestLog.Time.C,
                    Data = UnsafeByteOperations.UnsafeWrap(requestLog.LogData)
                });
            }

            BuildCount++;
            _logs = logs;
            return _logs;
        }
    }
}
