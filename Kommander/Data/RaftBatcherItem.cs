
namespace Kommander.Data;

public sealed class RaftBatcherItem
{
    /// <summary>
    /// List of logs to store
    /// </summary>
    public (int, List<RaftLog>) Request { get; }

    /// <summary>
    /// Returns the task completion source of the reply
    /// </summary>
    public TaskCompletionSource<RaftOperationStatus> Promise { get; }    

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="id"></param>
    /// <param name="request"></param>
    /// <param name="promise"></param>
    public RaftBatcherItem((int, List<RaftLog>) request, TaskCompletionSource<RaftOperationStatus> promise)
    {
        Request = request;
        Promise = promise;
    }
}