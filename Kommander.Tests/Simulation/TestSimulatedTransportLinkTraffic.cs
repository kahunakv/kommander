using Kommander.Data;
using Kommander.Tests.Simulation.Transport;

namespace Kommander.Tests.Simulation;

/// <summary>
/// The per-kind link filter must see through a batch.
///
/// <para><c>RaftTransportDispatcher</c> coalesces whatever is queued for one peer into one
/// <c>BatchRequests</c> frame, so a vote request rides in a batch whenever another message to the
/// same peer is queued at the same moment. A filter that judged the frame by its own kind let a
/// batched vote request through a <see cref="LinkTraffic.NoVoteRequests"/> link, and the straddler
/// of <see cref="TestStraddlingVoterScenarios"/> won the term the scenario forbids it (GA run
/// 39526417260, 2026-10-21). Whether two messages share a frame is decided by the dispatcher's
/// thread, outside simulated time, so a cluster test cannot force the batch. These tests pin the
/// per-item decision directly.</para>
/// </summary>
public sealed class TestSimulatedTransportLinkTraffic
{
    private static BatchRequestsRequestItem Item(BatchRequestsRequestType type) => new() { Type = type };

    private static BatchRequestsRequest Batch(params BatchRequestsRequestType[] types) =>
        new() { Requests = types.Select(Item).ToList() };

    private static List<BatchRequestsRequestType> Kinds(BatchRequestsRequest? batch) =>
        (batch?.Requests ?? []).Select(item => item.Type).ToList();

    [Fact]
    public void NoVoteRequests_DropsOnlyTheVoteRequestsInsideABatch()
    {
        BatchRequestsRequest batch = Batch(
            BatchRequestsRequestType.RequestVote,
            BatchRequestsRequestType.Vote,
            BatchRequestsRequestType.RequestVote,
            BatchRequestsRequestType.CompleteAppendLogs);

        BatchRequestsRequest? carried = SimulatedTransport.FilterByLinkTraffic(LinkTraffic.NoVoteRequests, batch, out int dropped);

        Assert.Equal(2, dropped);
        Assert.Equal([BatchRequestsRequestType.Vote, BatchRequestsRequestType.CompleteAppendLogs], Kinds(carried));
    }

    [Fact]
    public void ElectionOnly_KeepsOnlyTheElectionItemsInsideABatch()
    {
        BatchRequestsRequest batch = Batch(
            BatchRequestsRequestType.AppendLogs,
            BatchRequestsRequestType.RequestVote,
            BatchRequestsRequestType.CompleteAppendLogs,
            BatchRequestsRequestType.Vote,
            BatchRequestsRequestType.Handshake);

        BatchRequestsRequest? carried = SimulatedTransport.FilterByLinkTraffic(LinkTraffic.ElectionOnly, batch, out int dropped);

        Assert.Equal(3, dropped);
        Assert.Equal([BatchRequestsRequestType.RequestVote, BatchRequestsRequestType.Vote], Kinds(carried));
    }

    [Fact]
    public void ABatchWithNoCarriedItem_IsDroppedWhole()
    {
        BatchRequestsRequest batch = Batch(BatchRequestsRequestType.RequestVote, BatchRequestsRequestType.RequestVote);

        BatchRequestsRequest? carried = SimulatedTransport.FilterByLinkTraffic(LinkTraffic.NoVoteRequests, batch, out int dropped);

        Assert.Null(carried);
        Assert.Equal(2, dropped);
    }

    [Fact]
    public void ABatchTheLinkCarriesInFull_IsPassedUntouched()
    {
        BatchRequestsRequest batch = Batch(BatchRequestsRequestType.AppendLogs, BatchRequestsRequestType.Vote);

        BatchRequestsRequest? carriedByNoVoteRequests = SimulatedTransport.FilterByLinkTraffic(LinkTraffic.NoVoteRequests, batch, out int droppedByNoVoteRequests);
        BatchRequestsRequest? carriedByAll = SimulatedTransport.FilterByLinkTraffic(LinkTraffic.All, Batch(BatchRequestsRequestType.RequestVote), out int droppedByAll);

        Assert.Same(batch, carriedByNoVoteRequests);
        Assert.Equal(0, droppedByNoVoteRequests);
        Assert.NotNull(carriedByAll);
        Assert.Equal(0, droppedByAll);
    }

    [Fact]
    public void AnEmptyBatch_IsNotDropped()
    {
        BatchRequestsRequest empty = new();

        BatchRequestsRequest? carried = SimulatedTransport.FilterByLinkTraffic(LinkTraffic.ElectionOnly, empty, out int dropped);

        Assert.Same(empty, carried);
        Assert.Equal(0, dropped);
    }
}
