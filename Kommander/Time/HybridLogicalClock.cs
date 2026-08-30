using System.Runtime.CompilerServices;

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
/// <summary>

public sealed class HybridLogicalClock : IDisposable
{
    // ── Packed state ──────────────────────────────────────────────────────────
    //
    // (L, C) is packed into ONE long — L in the high 42 bits, C in the low 22 — so every clock
    // event is a single lock-free CAS on a long with ZERO allocation. The previous representation
    // (an immutable ClockState reference swapped by CAS) allocated a gen-0 object per event
    // (including per CAS retry), and this clock is shared by every partition: one event fires per
    // AppendLogs sent, per ack received, per propose, per heartbeat — making it the largest
    // steady-state allocation source in replication.
    //
    // Width reasoning:
    //  * L is unix-epoch milliseconds: 2^42 ms ≈ year 2109. MaxL guards the unreachable overflow
    //    (a wildly wrong system clock) with the same RaftException the corruption check uses.
    //  * C resets to 0 whenever L advances, so 22 bits allows ~4.19M events within one physical
    //    millisecond before overflow — orders of magnitude beyond reachable throughput. A remote
    //    message CAN carry an arbitrary large C (it is a uint on the wire), so counter overflow is
    //    handled, not assumed away: the clock rolls to (L+1, 0), which is strictly greater than
    //    both the local state and the message in HLC order — causality holds, and L running ahead
    //    of physical time is what HLC's logical component exists to tolerate.
    //  * The packed long is used ONLY for CAS identity, never compared/ordered, so the sign bit
    //    carries no meaning and unpacking uses the unsigned shift.
    //
    // Reads use Volatile.Read: unlike the old reference field, a plain long read may tear on
    // 32-bit platforms, and the acquire fence preserves the old reference-swap visibility.

    private const int CounterBits = 22;

    private const long CounterMask = (1L << CounterBits) - 1;

    /// <summary>Exclusive upper bound on the physical/logical component (fits year ~2109).</summary>
    private const long MaxL = 1L << (64 - CounterBits);

    private long _state;

    /// <summary>
    /// Supplies the physical component in Unix milliseconds. Defaults to the wall clock.
    ///
    /// <para>Injectable for one reason: a deterministic simulation must be able to make the whole
    /// node a function of simulated time. The hybrid logical clock is the last thing in a node
    /// that reads real time on its own, and its value reaches log entries and freshness gates, so
    /// leaving it on the wall clock would keep a run irreproducible however carefully everything
    /// else was driven.</para>
    ///
    /// <para>Production must leave it null. The physical component is what keeps a hybrid logical
    /// clock close to real time, which is the whole reason it is hybrid.</para>
    /// </summary>
    private readonly Func<long>? _physicalTimeMilliseconds;

    public HybridLogicalClock(Func<long>? physicalTimeMilliseconds = null)
    {
        _physicalTimeMilliseconds = physicalTimeMilliseconds;
        _state = Pack(GetPhysicalTime(), 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long Pack(long l, long c)
    {
        if (l is <= 0 or >= MaxL)
            throw new RaftException("Corrupted HLC clock");

        return (l << CounterBits) | c;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long UnpackL(long state) => state >>> CounterBits;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint UnpackC(long state) => (uint)(state & CounterMask);

    /// <summary>
    /// Applies the counter-overflow rollover: an increment that no longer fits the packed counter
    /// field advances L by one and resets C. (L+1, 0) is strictly greater than (L, anything) in
    /// HLC order, so causality is preserved; L transiently running ahead of physical time is
    /// normal HLC behavior and self-corrects once the wall clock catches up.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Normalize(ref long l, ref long c)
    {
        if (c > CounterMask)
        {
            l += 1;
            c = 0;
        }
    }

    /// <summary>
    /// Handles a send or local event by updating the clock state atomically.
    /// </summary>
    public HLCTimestamp SendOrLocalEvent(int nodeId)
    {
        while (true)
        {
            long current = Volatile.Read(ref _state);
            long currentL = UnpackL(current);
            long physicalTime = GetPhysicalTime();
            long newL = Math.Max(currentL, physicalTime);
            long newC = (newL == currentL) ? (long)UnpackC(current) + 1 : 0;
            Normalize(ref newL, ref newC);

            if (Interlocked.CompareExchange(ref _state, Pack(newL, newC), current) == current)
                return new(nodeId, newL, (uint)newC);

            // Otherwise, another thread has updated the state; try again.
        }
    }

    /// <summary>
    /// Mints and installs a new timestamp, exactly like <see cref="SendOrLocalEvent"/>.
    /// Historically this method attempted ONE CAS and, on failure, returned a computed-but-not-installed
    /// timestamp. That is unsound: a stamp the clock never recorded can be minted AGAIN by the next
    /// event (the loser computes (L, C+1) from the updated state, then another thread CASes the same
    /// (L, C+1) in), producing duplicate timestamps under contention. Consumers that order same-key
    /// mutations by timestamp (e.g. Kahuna's lock cache-coherence guard, which compares with &gt;=)
    /// then drop the later mutation — a released lock resurfacing as held. Every returned stamp must
    /// therefore be installed, which requires retrying the CAS until it lands.
    /// </summary>
    public HLCTimestamp TrySendOrLocalEvent(int nodeId)
    {
        return SendOrLocalEvent(nodeId);
    }

    /// <summary>
    /// Handles a receive event given a message timestamp.
    /// </summary>
    public HLCTimestamp ReceiveEvent(int nodeId, HLClockMessage m)
    {
        if (m.L == 0)
            throw new RaftException("Invalid HLC timestamp argument");

        return ReceiveEventInternal(nodeId, m.L, m.C);
    }

    /// <summary>
    /// Handles a receive event given a timestamp.
    /// </summary>
    public HLCTimestamp ReceiveEvent(int nodeId, HLCTimestamp m)
    {
        if (m.L == 0)
            throw new RaftException("Invalid HLC timestamp argument");

        return ReceiveEventInternal(nodeId, m.L, m.C);
    }

    private HLCTimestamp ReceiveEventInternal(int nodeId, long messageL, uint messageC)
    {
        while (true)
        {
            long current = Volatile.Read(ref _state);
            long currentL = UnpackL(current);
            uint currentC = UnpackC(current);
            long physicalTime = GetPhysicalTime();
            long newL = Math.Max(currentL, Math.Max(messageL, physicalTime));
            long newC;

            if (newL == currentL && newL == messageL)
                newC = (long)Math.Max(currentC, messageC) + 1;
            else if (newL == currentL)
                newC = (long)currentC + 1;
            else if (newL == messageL)
                newC = (long)messageC + 1;
            else
                newC = 0;

            Normalize(ref newL, ref newC);

            if (Interlocked.CompareExchange(ref _state, Pack(newL, newC), current) == current)
                return new(nodeId, newL, (uint)newC);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetPhysicalTime()
    {
        return _physicalTimeMilliseconds is null
            ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            : _physicalTimeMilliseconds();
    }

    public void Dispose()
    {
        // No disposable resources need to be released here.
    }
}
