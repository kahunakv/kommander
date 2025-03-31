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
    // Immutable state holder for the clock.
    private sealed class ClockState
    {
        public readonly long L;  // Logical (or physical) clock value
        public readonly uint C;  // Counter

        public ClockState(long l, uint c)
        {
            L = l;
            C = c;
        }
    }

    // The clock state is stored as an atomic reference.
    private ClockState _state;

    public HybridLogicalClock()
    {
        long initialTime = GetPhysicalTime();
        _state = new(initialTime, 0);
    }

    /// <summary>
    /// Handles a send or local event by updating the clock state atomically.
    /// </summary>
    public HLCTimestamp SendOrLocalEvent()
    {
        while (true)
        {
            ClockState current = _state;
            long physicalTime = GetPhysicalTime();
            long newL = Math.Max(current.L, physicalTime);
            uint newC = (newL == current.L) ? current.C + 1 : 0;

            if (newL == 0)
                throw new RaftException("Corrupted HLC clock");

            ClockState newState = new(newL, newC);
            
            // Try to atomically update the state.
            if (Interlocked.CompareExchange(ref _state, newState, current) == current)
                return new(newL, newC);
            
            // Otherwise, another thread has updated the state; try again.
        }
    }

    /// <summary>
    /// If it succeeds, it returns the new timestamp and updates the global clock. If it fails, it immediately
    /// reads the latest state and computes a timestamp that is guaranteed to be higher than
    /// the current state even though it does not update the state itself.
    /// </summary>
    public HLCTimestamp TrySendOrLocalEvent()
    {
        // Read the current state.
        ClockState current = _state;
        long physicalTime = GetPhysicalTime();
        long candidateL = Math.Max(current.L, physicalTime);
        uint candidateC = (candidateL == current.L) ? current.C + 1 : 0;
        ClockState candidate = new(candidateL, candidateC);

        // Attempt one CAS update.
        if (Interlocked.CompareExchange(ref _state, candidate, current) == current)
            return new(candidateL, candidateC); // Successfully updated state.
        
        // CAS failed; get the latest state and compute a timestamp that's higher.
        ClockState updatedState = _state; // Read the updated state.
        long newL = Math.Max(updatedState.L, physicalTime);
        uint newC = (newL == updatedState.L) ? updatedState.C + 1 : 0;
        return new(newL, newC);
    }

    /// <summary>
    /// Handles a receive event given a message timestamp.
    /// </summary>
    public HLCTimestamp ReceiveEvent(HLClockMessage m)
    {
        if (m.L == 0)
            throw new RaftException("Invalid HLC timestamp argument");

        while (true)
        {
            ClockState current = _state;
            long physicalTime = GetPhysicalTime();
            long newL = Math.Max(current.L, Math.Max(m.L, physicalTime));
            uint newC;

            if (newL == current.L && newL == m.L)
                newC = Math.Max(current.C, m.C) + 1;
            else if (newL == current.L)
                newC = current.C + 1;
            else if (newL == m.L)
                newC = m.C + 1;
            else
                newC = 0;

            if (newL == 0)
                throw new RaftException("Corrupted HLC clock");

            ClockState newState = new(newL, newC);
            
            if (Interlocked.CompareExchange(ref _state, newState, current) == current)
                return new(newL, newC);
        }
    }

    /// <summary>
    /// Handles a receive event given a timestamp.
    /// </summary>
    public HLCTimestamp ReceiveEvent(HLCTimestamp m)
    {
        if (m.L == 0)
            throw new RaftException("Invalid HLC timestamp argument");

        while (true)
        {
            ClockState current = _state;
            long physicalTime = GetPhysicalTime();
            long newL = Math.Max(current.L, Math.Max(m.L, physicalTime));
            uint newC;

            if (newL == current.L && newL == m.L)
                newC = Math.Max(current.C, m.C) + 1;
            else if (newL == current.L)
                newC = current.C + 1;
            else if (newL == m.L)
                newC = m.C + 1;
            else
                newC = 0;

            if (newL == 0)
                throw new RaftException("Corrupted HLC clock");

            ClockState newState = new(newL, newC);
            if (Interlocked.CompareExchange(ref _state, newState, current) == current)
                return new(newL, newC);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetPhysicalTime()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public void Dispose()
    {
        // No disposable resources need to be released here.
    }
}
