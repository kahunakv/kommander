
namespace Kommander.WAL;

/// <summary>
/// The metadata key and value encoding of the per-partition Raft hard state (term and vote), shared by
/// the <see cref="IWAL"/> defaults and by backends that mirror the hard state elsewhere
/// (<see cref="RocksDbWAL"/>, whose hard-state ledger must write exactly what the engine holds).
/// </summary>
internal static class RaftHardStateCodec
{
    public static string Key(int partitionId) => $"raft_hardstate_p{partitionId}";

    /// <summary>
    /// "term" with no vote, or "term\nvotedFor". The separator is omitted when there is no vote so the
    /// value never ends in a trailing '\n' — some backends (SQLite) strip trailing newlines on read, which
    /// would otherwise corrupt the round-trip. Endpoints never contain '\n'.
    /// </summary>
    public static string Encode(long currentTerm, string? votedFor) =>
        string.IsNullOrEmpty(votedFor) ? currentTerm.ToString() : $"{currentTerm}\n{votedFor}";

    public static bool TryDecode(string? raw, out long currentTerm, out string? votedFor)
    {
        currentTerm = 0;
        votedFor = null;

        if (string.IsNullOrEmpty(raw))
            return false;

        int newline = raw.IndexOf('\n');
        if (newline < 0)
        {
            // No separator → term only, no recorded vote (also covers backends that strip a trailing '\n').
            return long.TryParse(raw, out currentTerm);
        }

        if (!long.TryParse(raw.AsSpan(0, newline), out currentTerm))
            return false;

        string vote = raw[(newline + 1)..];
        votedFor = string.IsNullOrEmpty(vote) ? null : vote;
        return true;
    }
}
