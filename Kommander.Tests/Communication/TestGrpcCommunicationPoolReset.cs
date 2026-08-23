using Google.Protobuf;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Pins the pool-return reset contract for the pooled gRPC wire messages: a returned object must
/// come back from the pool with EVERY field at its wire default, so pool safety never depends on
/// the next renter overwriting every scalar before send. (The pools are process-wide statics, so
/// each test drains cross-talk by asserting on the exact instance it returned.)
/// </summary>
public class TestGrpcCommunicationPoolReset
{
    [Fact]
    public void ReturnedAppendLogsRequest_ComesBackFullyReset()
    {
        GrpcAppendLogsRequest obj = GrpcCommunicationPool.RentAppendLogsRequest();

        obj.Partition = 42;
        obj.Term = 7;
        obj.TimeNode = 1;
        obj.TimePhysical = 123456;
        obj.TimeCounter = 9;
        obj.Endpoint = "node-x:8100";
        obj.PrevLogIndex = 55;
        obj.PrevLogTerm = 6;
        obj.Quiesce = true;
        obj.Logs.Add(new GrpcRaftLog { Id = 1, Term = 7, Data = ByteString.CopyFrom([1, 2, 3]) });

        GrpcCommunicationPool.Return(obj);

        // Rent until the same instance comes back (bounded pool, other tests may interleave).
        GrpcAppendLogsRequest rented = RentUntilSame(obj, GrpcCommunicationPool.RentAppendLogsRequest, GrpcCommunicationPool.Return);

        Assert.Equal(0, rented.Partition);
        Assert.Equal(0, rented.Term);
        Assert.Equal(0, rented.TimeNode);
        Assert.Equal(0, rented.TimePhysical);
        Assert.Equal(0u, rented.TimeCounter);
        Assert.Equal("", rented.Endpoint);
        Assert.Equal(0, rented.PrevLogIndex);
        Assert.Equal(0, rented.PrevLogTerm);
        Assert.False(rented.Quiesce);
        Assert.Empty(rented.Logs);
    }

    [Fact]
    public void ReturnedCompleteAppendLogsRequest_ComesBackFullyReset()
    {
        GrpcCompleteAppendLogsRequest obj = GrpcCommunicationPool.RentCompleteAppendLogsRequest();

        obj.Partition = 42;
        obj.Term = 7;
        obj.TimeNode = 1;
        obj.TimePhysical = 123456;
        obj.TimeCounter = 9;
        obj.Endpoint = "node-x:8100";
        obj.Status = GrpcRaftOperationStatus.Errored;
        obj.CommitIndex = 88;

        GrpcCommunicationPool.Return(obj);

        GrpcCompleteAppendLogsRequest rented = RentUntilSame(obj, GrpcCommunicationPool.RentCompleteAppendLogsRequest, GrpcCommunicationPool.Return);

        Assert.Equal(0, rented.Partition);
        Assert.Equal(0, rented.Term);
        Assert.Equal(0, rented.TimeNode);
        Assert.Equal(0, rented.TimePhysical);
        Assert.Equal(0u, rented.TimeCounter);
        Assert.Equal("", rented.Endpoint);
        Assert.Equal(default, rented.Status);
        Assert.Equal(0, rented.CommitIndex);
    }

    /// <summary>
    /// Rents until <paramref name="expected"/> reappears, then returns every other instance it
    /// pulled out along the way. Bounded: gives up (and fails the assertion) after the pool cap.
    /// </summary>
    private static T RentUntilSame<T>(T expected, Func<T> rent, Action<T> giveBack) where T : class
    {
        List<T> others = [];
        try
        {
            for (int i = 0; i < 1024; i++)
            {
                T candidate = rent();
                if (ReferenceEquals(candidate, expected))
                    return candidate;
                others.Add(candidate);
            }

            Assert.Fail("Returned instance never came back from the pool.");
            throw new UnreachableException();
        }
        finally
        {
            foreach (T other in others)
                giveBack(other);
        }
    }

    private sealed class UnreachableException : Exception;
}
