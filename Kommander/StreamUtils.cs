
namespace Kommander;

/// <summary>
/// Low-level stream helpers shared across subsystems that perform binary framing or
/// snapshot transfer. Methods here have no Raft-state dependency and are safe to call
/// from any thread.
/// </summary>
internal static class StreamUtils
{
    /// <summary>
    /// Reads up to <paramref name="count"/> bytes from <paramref name="stream"/> into
    /// <paramref name="buffer"/>, issuing repeated reads until the buffer is full or the
    /// stream is exhausted. Returns the total number of bytes read.
    /// </summary>
    internal static async ValueTask<int> ReadExactAsync(Stream stream, byte[] buffer, int count, CancellationToken ct)
    {
        int total = 0;
        while (total < count)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(total, count - total), ct).ConfigureAwait(false);
            if (n == 0) break;
            total += n;
        }
        return total;
    }
}
