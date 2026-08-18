namespace Kommander.Consensus;

/// <summary>
/// Peer-roster lookups shared by the partition state machine and its collaborators.
/// </summary>
internal static class RaftPeers
{
    /// <summary>
    /// Finds a node by endpoint with an index loop rather than LINQ: this runs on the per-ack and
    /// per-heartbeat paths, where an enumerator allocation per lookup is measurable.
    /// </summary>
    public static RaftNode? FindByEndpoint(IReadOnlyList<RaftNode> nodes, string endpoint)
    {
        for (int i = 0; i < nodes.Count; i++)
        {
            if (nodes[i].Endpoint == endpoint)
                return nodes[i];
        }

        return null;
    }
}
