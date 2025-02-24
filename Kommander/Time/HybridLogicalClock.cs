namespace Kommander.Time;

/// <summary>
/// A Hybrid Logical Clock (HLC) is an algorithm used in distributed systems to track the order of events.
/// It combines elements of both physical clocks and logical clocks to achieve a more accurate and consistent
/// ordering of events across different nodes in a distributed system.
///
/// - Physical Clocks: HLC uses the physical time from the system clocks of the machines in the network.However,
/// relying solely on physical clocks can lead to issues due to clock drift and synchronization problems.
///
/// - Logical Clocks: To address the limitations of physical clocks, HLC also incorporates a logical component.
/// Logical clocks are a method of ordering events based on the causality relationship rather than actual time.
/// They increment with each event, ensuring a unique and consistent order.
///
/// - Hybrid Approach: HLC merges these two approaches. It uses physical time when possible to keep the logical
/// clock close to real time.However, when the physical clock is behind the logical clock (due to clock drift
/// or other reasons), the logical component of the HLC advances to maintain the order.
///
/// - Event Ordering: In a distributed system, when a message is sent from one node to another, the HLC timestamp
/// of the sender is sent along with the message.The receiving node then adjusts its HLC based on the received timestamp,
/// ensuring a consistent and ordered view of events across the system.
///
/// - Advantages: HLC provides a more accurate representation of time in distributed systems compared to purely
/// logical clocks. It ensures causality and can approximate real-time more closely, making it useful for systems
/// where time ordering is crucial.
/// </summary>
/// <see cref="https://cse.buffalo.edu/~demirbas/publications/hlc.pdf"/>
/// <see cref="https://raw.githubusercontent.com/camusdb/camusdb/refs/heads/main/CamusDB.Core/Util/Time/HybridLogicalClock.cs"/>
public sealed class HybridLogicalClock : IDisposable
{
    private long l; // logical clock

    private uint c;  // counter

    private readonly SemaphoreSlim semaphore = new(1, 1);

    /// <summary>
    /// Call this method when a send or local event occurs
    /// </summary>
    /// <returns></returns>
    public async Task<HLCTimestamp> SendOrLocalEvent()
    {
        try
        {
            await semaphore.WaitAsync().ConfigureAwait(false);

            long lPrime = l;

            l = Math.Max(l, GetPhysicalTime());

            if (l == lPrime)
                c++;
            else
                c = 0;

            return new(l, c);
        }
        finally
        {
            semaphore.Release();
        }
    }

    /// <summary>
    /// Call this method when a receive event occurs `m` represents the message received,
    /// which should contain its own timestamp
    /// </summary>
    /// <param name="m"></param>
    /// <returns></returns>
    public async Task<HLCTimestamp> ReceiveEvent(HLClockMessage m)
    {
        try
        {
            await semaphore.WaitAsync();

            long lPrime = l;

            l = Math.Max(l, Math.Max(m.L, GetPhysicalTime()));

            if (l == lPrime && l == m.L)
                c = Math.Max(c, m.C) + 1;
            else if (l == lPrime)
                c += 1;
            else if (l == m.L)
                c = m.C + 1;
            else
                c = 0;

            return new(l, c);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    /// <summary>
    /// Call this method when a receive event occurs `m` represents the message received,
    /// which should contain its own timestamp
    /// </summary>
    /// <param name="m"></param>
    /// <returns></returns>
    public async Task<HLCTimestamp> ReceiveEvent(HLCTimestamp m)
    {
        try
        {
            await semaphore.WaitAsync();

            long lPrime = l;

            l = Math.Max(l, Math.Max(m.L, GetPhysicalTime()));

            if (l == lPrime && l == m.L)
                c = Math.Max(c, m.C) + 1;
            else if (l == lPrime)
                c += 1;
            else if (l == m.L)
                c = m.C + 1;
            else
                c = 0;

            return new(l, c);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private static long GetPhysicalTime()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public void Dispose()
    {
        semaphore.Dispose();
    }
}