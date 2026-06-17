using Kommander.Communication.Grpc;
using Kommander.Data;

namespace Kommander.Tests.Communication;

public sealed class TestAppendLogsGrpcLogCache
{
    [Fact]
    public void GetOrCreate_BuildsProtobufLogsOncePerBatch()
    {
        AppendLogsGrpcLogCache cache = new();
        List<RaftLog> logs =
        [
            new()
            {
                Id = 1,
                Term = 2,
                Type = RaftLogType.Proposed,
                LogType = "test",
                LogData = [1, 2, 3]
            },
            new()
            {
                Id = 2,
                Term = 2,
                Type = RaftLogType.Proposed,
                LogType = "test",
                LogData = [4, 5]
            }
        ];

        var first = cache.GetOrCreate(logs);
        var second = cache.GetOrCreate(logs);

        Assert.Same(first, second);
        Assert.Equal(2, first.Count);
        Assert.Equal(1, cache.BuildCount);
    }

    [Fact]
    public void GetOrCreate_IsSafeUnderConcurrentFollowerSends()
    {
        AppendLogsGrpcLogCache cache = new();
        List<RaftLog> logs =
        [
            new()
            {
                Id = 1,
                Term = 1,
                Type = RaftLogType.Committed,
                LogType = "test",
                LogData = [9]
            }
        ];

        Google.Protobuf.Collections.RepeatedField<GrpcRaftLog>[] results = new Google.Protobuf.Collections.RepeatedField<GrpcRaftLog>[8];

        Parallel.For(0, results.Length, i => results[i] = cache.GetOrCreate(logs));

        Assert.All(results, field => Assert.Same(results[0], field));
        Assert.Equal(1, cache.BuildCount);
    }
}
