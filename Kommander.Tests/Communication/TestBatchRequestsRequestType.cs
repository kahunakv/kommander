using Kommander.Data;

namespace Kommander.Tests.Communication;

public sealed class TestBatchRequestsRequestType
{
    [Fact]
    public void BatchRequestsRequestType_PreservesWireCompatibleNumericValues()
    {
        Assert.Equal(0, (int)BatchRequestsRequestType.Ping);
        Assert.Equal(1, (int)BatchRequestsRequestType.Handshake);
        Assert.Equal(2, (int)BatchRequestsRequestType.Vote);
        Assert.Equal(3, (int)BatchRequestsRequestType.RequestVote);
        Assert.Equal(4, (int)BatchRequestsRequestType.AppendLogs);
        Assert.Equal(5, (int)BatchRequestsRequestType.CompleteAppendLogs);
        Assert.Equal(6, (int)BatchRequestsRequestType.StepDownNotice);
        Assert.Equal(7, (int)BatchRequestsRequestType.TransferLeadership);
    }
}
