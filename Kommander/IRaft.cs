
using Kommander.Data;
using Kommander.WAL;
using Kommander.Communication;

namespace Kommander;

public interface IRaft
{
    public IWAL WalAdapter { get; }
    
    public ICommunication Communication { get; }
    
    public RaftConfiguration Configuration { get; }
    
    public Task JoinCluster();

    public Task UpdateNodes();

    public void RequestVote(RequestVotesRequest request);

    public void Vote(VoteRequest request);

    public Task<long> AppendLogs(AppendLogsRequest request);

    public void ReplicateLogs(int partitionId, string message);

    public void ReplicateCheckpoint(int partitionId);

    public string GetLocalEndpoint();

    public ValueTask<bool> AmILeaderQuick(int partitionId);

    public ValueTask<bool> AmILeader(int partitionId);

    public ValueTask<string> WaitForLeader(int partitionId);
}