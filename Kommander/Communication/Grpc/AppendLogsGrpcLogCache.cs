using Google.Protobuf;
using Google.Protobuf.Collections;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Cached protobuf log entries for one Raft batch fanned out to multiple followers.
/// Built once per replication round; gRPC copies references into each outbound request.
/// </summary>
public sealed class AppendLogsGrpcLogCache
{
    private readonly object _lock = new();

    private RepeatedField<GrpcRaftLog>? _logs;

    /// <summary>For tests: how many times the protobuf log list was materialized.</summary>
    internal int BuildCount { get; private set; }

    /// <summary>
    /// Returns the cached <see cref="GrpcRaftLog"/> list for <paramref name="source"/>,
    /// building it on first use. Thread-safe for concurrent follower sends.
    /// </summary>
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
