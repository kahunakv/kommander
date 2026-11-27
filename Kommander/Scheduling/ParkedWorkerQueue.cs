using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Kommander.Scheduling;

/// <summary>Defaults shared by every <see cref="ParkedWorkerQueue{T}"/>.</summary>
internal static class ParkedWorkerQueue
{
    /// <summary>Default upper bound of the adaptive spin window, the one the read scheduler uses.</summary>
    public static readonly TimeSpan DefaultMaxSpinBeforePark = TimeSpan.FromMicroseconds(20);
}

/// <summary>
/// A ready queue served by a fixed set of dedicated worker threads, with the idle-worker handshake
/// <see cref="WAL.IO.FairReadScheduler"/> introduced: a lock-free queue, a stack of parked workers each
/// on its own event, one wake per item, and a bounded adaptive spin by at most one worker.
///
/// <para><b>Why it exists.</b> <see cref="RaftExecutorPool"/> parked its workers on one
/// <c>SemaphoreSlim</c> and <see cref="WAL.IO.FairWalScheduler"/> took from a <c>BlockingCollection</c>.
/// Both spin before they park, on every worker and at every wait, and a cancellable wait registers
/// with its token each time. A profile of one node at 4,800 small rounds a second found those two pools
/// at 20% of the process CPU, two thirds of it waiting for the next item. Here an idle worker costs
/// nothing while it is parked, and a producer wakes exactly one of them.</para>
///
/// <para><b>The wake invariant.</b> While the queue holds an item, at least one of these is true: a
/// worker was woken for it, a spinning worker will find it, or the worker that published it is about
/// to return to the queue. Each producer path keeps it:</para>
/// <list type="bullet">
///   <item><see cref="Enqueue"/> pops one parked worker and sets its event, unless a worker is
///       spinning (the spinner takes the item with no kernel wake) or a worker waits in
///       <see cref="TryTake"/> (it is woken first).</item>
///   <item><see cref="EnqueueFromWorker"/> is for a worker that returns to <see cref="Take"/> right
///       after: it takes the item itself.</item>
///   <item><see cref="Publish"/> wakes nobody; its caller owes a <see cref="WakeOne"/>.</item>
///   <item>A worker that takes one item and leaves others behind calls <see cref="HandOnIfQueued"/>
///       (or passes <c>handOn</c> to <see cref="Take"/>), so wakes that producers skipped because of
///       the spinner are passed on, one worker per remaining item.</item>
/// </list>
/// <para>See <see cref="Park"/> for why a worker cannot miss an item on its way to sleep.</para>
///
/// <para><b>Threads.</b> Any thread may call <see cref="Enqueue"/>, <see cref="Publish"/>,
/// <see cref="TryDequeue"/>, <see cref="WakeOne"/> and <see cref="HandOnIfQueued"/>. <see cref="Take"/> and <see cref="TryTake"/>
/// belong to the worker that owns the slot index it passes; two threads must never share an index.
/// With zero workers (a caller-driven owner, or the thread-free build) only the queue is used and no
/// call here blocks.</para>
/// </summary>
internal sealed class ParkedWorkerQueue<T> : IDisposable
{
    private readonly ConcurrentQueue<T> _queue = new();

    /// <summary>
    /// One per worker thread. A slot is on the idle stack exactly while its worker is parked or about
    /// to park, and only the thread that pops it sets its event, so a set is never lost or doubled.
    /// </summary>
    private sealed class WorkerSlot : IDisposable
    {
        internal readonly int Index;

        /// <summary>Spin count zero: <c>Wait</c> goes straight to the kernel. No spin on park or on wake.</summary>
        internal readonly ManualResetEventSlim Event = new(false, spinCount: 0);

        /// <summary>Index of the slot below this one on the idle stack, or -1.</summary>
        internal int Next = -1;

        internal WorkerSlot(int index) => Index = index;

        public void Dispose() => Event.Dispose();
    }

    private readonly WorkerSlot[] _slots;

    /// <summary>
    /// Head of the idle-worker stack (a Treiber stack over <see cref="_slots"/>). Low 32 bits: top slot
    /// index + 1 (0 = empty). High 32 bits: a version bumped by every push and pop, so a pop that read
    /// a stale <see cref="WorkerSlot.Next"/> (the slot was popped and pushed again meanwhile, which
    /// reused slots make possible) fails its compare-exchange instead of corrupting the stack.
    /// </summary>
    private long _idleHead;

    /// <summary>
    /// Upper bound, in <see cref="Stopwatch"/> ticks, of how long ONE idle worker (the spinner) polls the
    /// queue before it parks. Zero disables spinning: every idle worker parks at once and every
    /// hand-off is a kernel wake.
    ///
    /// <para>A spin pays only when the next item arrives inside the window, so the window adapts
    /// (<see cref="_spinWindowTicks"/>) and at most one worker spins at a time. A slow stream closes the
    /// window after a few misses; a fast one keeps one worker polling and its producers skip the wake
    /// system call.</para>
    /// </summary>
    private readonly long maxSpinTicks;

    /// <summary>
    /// Current spin window, 0..<see cref="maxSpinTicks"/>. A spin that finds nothing halves it (to zero
    /// below a sixteenth of the bound); a park that a producer ends sooner than the bound doubles it
    /// (from zero to a quarter of the bound), because a spin of that length would have caught the item.
    /// Plain racy updates: it is a heuristic and never decides whether an item is served.
    ///
    /// <para><b>A hit leaves it alone, on purpose.</b> The rule is the read scheduler's. With hand-offs
    /// only a few microseconds apart it has two stable states: the window open and every hand-off
    /// caught, or the window collapsed, where each short park doubles it and the too-short spin that
    /// follows halves it again. The serial one-voter benchmark shows both (51,000 or 88,000 proposals a
    /// second, run to run). Widening on a hit removes the slow state, but it also keeps the window open
    /// for a stream of bursts with gaps between them, which then pays one wasted spin per burst: measured
    /// at 4,750 rounds a second, +0.08 core per executor pool for no shorter round. The collapsed state
    /// costs the least CPU and is never slower than the semaphore it replaced, so it stays. A host that
    /// wants the fast state every time raises the bound.</para>
    /// </summary>
    private long _spinWindowTicks;

#if KOMMANDER_THREAD_FREE
    // The thread-free build has no workers to wake, so these two stay zero there.
#pragma warning disable CS0649
#endif
    private long _wakeups;
    private long _emptyWakeups;
#if KOMMANDER_THREAD_FREE
#pragma warning restore CS0649
#endif

#if !KOMMANDER_THREAD_FREE
    /// <summary>
    /// 1 while a worker is the spinner. A producer that reads 1 skips the wake. The skip does not have
    /// to be right: see <see cref="TrySpinForWork"/> and <see cref="Park"/>.
    /// </summary>
    private int _spinning;

    /// <summary>Set to 1, with a full fence, by <see cref="RequestStop"/>. Never cleared.</summary>
    private int _stopRequested;

    /// <summary>Guards <see cref="_timedWaiters"/>.</summary>
    private readonly object _timedLock = new();

    /// <summary>
    /// Workers inside <see cref="TryTake"/>. Kept apart from the idle stack because a timed waiter
    /// must be able to leave when its time is up, and a slot cannot be taken out of the middle of the
    /// lock-free stack. Used only by an owner that waits a bounded time for more items.
    /// </summary>
    private readonly List<WorkerSlot> _timedWaiters = [];

    /// <summary>Count of <see cref="_timedWaiters"/>, so a producer reads it without the lock.</summary>
    private int _timedWaiterCount;
#endif

    /// <param name="workerCount">Number of worker threads that will call <see cref="Take"/>; 0 for none.</param>
    /// <param name="maxSpinBeforePark">
    /// Upper bound of the adaptive spin window. Null means
    /// <see cref="ParkedWorkerQueue.DefaultMaxSpinBeforePark"/>; <see cref="TimeSpan.Zero"/> disables
    /// spinning.
    /// </param>
    public ParkedWorkerQueue(int workerCount, TimeSpan? maxSpinBeforePark = null)
    {
        TimeSpan spinBound = maxSpinBeforePark ?? ParkedWorkerQueue.DefaultMaxSpinBeforePark;
        maxSpinTicks = spinBound <= TimeSpan.Zero ? 0 : Math.Max(1, (long)(spinBound.TotalSeconds * Stopwatch.Frequency));
        _spinWindowTicks = maxSpinTicks;

        _slots = new WorkerSlot[Math.Max(0, workerCount)];

        for (int i = 0; i < _slots.Length; i++)
            _slots[i] = new WorkerSlot(i);
    }

    // ── Queue ──────────────────────────────────────────────────────────────

    /// <summary>True when no item is queued. A snapshot: a concurrent enqueue can change it at once.</summary>
    public bool IsEmpty => _queue.IsEmpty;

    /// <summary>
    /// Publishes <paramref name="item"/> and makes sure a worker comes for it (<see cref="WakeOne"/>).
    /// Never blocks: the wake is a lock-free pop and an event set.
    /// </summary>
    public void Enqueue(T item)
    {
        _queue.Enqueue(item);
        WakeOne();
    }

    /// <summary>
    /// Publishes <paramref name="item"/> and wakes nobody. The caller owes one <see cref="WakeOne"/>
    /// for it, after the publish: a caller that holds a lock the woken worker needs wakes once it has
    /// released it, and a caller that publishes several items a single worker will take together
    /// wakes once for all of them.
    /// </summary>
    public void Publish(T item) => _queue.Enqueue(item);

    /// <summary>
    /// Publishes <paramref name="item"/> from a worker of this queue that returns to
    /// <see cref="Take"/> right after. That worker takes the item itself, so no parked worker is
    /// woken: a wake there only produces a worker that finds nothing.
    ///
    /// <para>A worker in <see cref="TryTake"/> is still woken. It holds work and waits for more to
    /// arrive, and an item put back by another worker is such an arrival; left asleep, it would wait
    /// out its whole time for an item it could have taken at once.</para>
    /// </summary>
    public void EnqueueFromWorker(T item)
    {
        _queue.Enqueue(item);
        WakeTimedWaiter();
    }

    /// <summary>
    /// Wakes one worker waiting in <see cref="TryTake"/>, if there is one, and no other. For a caller
    /// that used <see cref="Publish"/> and returns to the queue itself.
    /// </summary>
    public void WakeTimedWaiter()
    {
#if !KOMMANDER_THREAD_FREE
        if (Volatile.Read(ref _timedWaiterCount) != 0)
            TryWakeTimedWaiter();
#endif
    }

    /// <summary>
    /// Wakes the worker that owns slot <paramref name="worker"/> if it waits in <see cref="TryTake"/>
    /// or <see cref="TryTakeWhile"/>, and nobody otherwise. For a caller that changed a condition this one worker waits for; see the
    /// <see cref="TryTakeWhile"/> for how the two cannot miss each other.
    ///
    /// <para>The untargeted <see cref="WakeTimedWaiter()"/> wakes the waiter that registered last,
    /// which is right for an item (any waiter can take it) and wrong for a condition: with two
    /// workers in a timed wait, the wake could go to the one that does not wait for it.</para>
    /// </summary>
    public void WakeTimedWaiter(int worker)
    {
#if !KOMMANDER_THREAD_FREE
        if (Volatile.Read(ref _timedWaiterCount) == 0)
            return;

        WorkerSlot slot = _slots[worker];

        lock (_timedLock)
        {
            if (!_timedWaiters.Remove(slot))
                return;

            Interlocked.Decrement(ref _timedWaiterCount);
            slot.Event.Set();
        }
#endif
    }

    /// <summary>Takes the oldest item without waiting. Any thread; wakes nobody.</summary>
    public bool TryDequeue([MaybeNullWhen(false)] out T item) => _queue.TryDequeue(out item);

    /// <summary>
    /// Brings one worker to the queue for an item already published: a worker waiting in
    /// <see cref="TryTake"/> first, else nobody when a worker is spinning, else one parked worker.
    /// When every worker is busy this does nothing, and one of them finds the item on its next take.
    /// </summary>
    public void WakeOne()
    {
#if !KOMMANDER_THREAD_FREE
        if (Volatile.Read(ref _timedWaiterCount) != 0 && TryWakeTimedWaiter())
            return;

        if (Volatile.Read(ref _spinning) != 0)
            return;
#endif

        WakeOneIdle();
    }

    /// <summary>
    /// Passes on the wakes producers skipped. Called by a worker that has taken what it will take:
    /// when items remain and no worker is spinning, one parked worker is woken for them, and it does the
    /// same after its own take.
    /// </summary>
    public void HandOnIfQueued()
    {
        if (!_queue.IsEmpty)
            WakeOne();
    }

    // ── Idle-worker stack ──────────────────────────────────────────────────

    private static long PackIdleHead(long previous, int slotIndex) =>
        unchecked((long)(((ulong)previous & 0xFFFF_FFFF_0000_0000UL) + 0x1_0000_0000UL) | (uint)(slotIndex + 1));

    private bool TryPopIdle(out WorkerSlot? slot)
    {
        while (true)
        {
            long head = Volatile.Read(ref _idleHead);
            int top = (int)(uint)head - 1;

            if (top < 0)
            {
                slot = null;
                return false;
            }

            WorkerSlot candidate = _slots[top];
            int next = Volatile.Read(ref candidate.Next);

            if (Interlocked.CompareExchange(ref _idleHead, PackIdleHead(head, next), head) == head)
            {
                slot = candidate;
                return true;
            }
        }
    }

    private void WakeOneIdle()
    {
        if (TryPopIdle(out WorkerSlot? slot))
            slot!.Event.Set();
    }

    private void WakeAllIdle()
    {
        while (TryPopIdle(out WorkerSlot? slot))
            slot!.Event.Set();
    }

    // ── Observability ──────────────────────────────────────────────────────

    /// <summary>Times a parked worker was woken by another thread.</summary>
    public long Wakeups => Interlocked.Read(ref _wakeups);

    /// <summary>Wake-ups after which the worker found the queue empty: another worker took the item first.</summary>
    public long EmptyWakeups => Interlocked.Read(ref _emptyWakeups);

    /// <summary>Test seam: the current adaptive spin window, in <see cref="Stopwatch"/> ticks.</summary>
    internal long SpinWindowTicks => Volatile.Read(ref _spinWindowTicks);

    /// <summary>
    /// Test seam: workers on the idle stack (parked, or pushed and about to park). Walks the stack without
    /// a lock, so it is exact only while no worker or producer is moving.
    /// </summary>
    internal int IdleWorkerCount
    {
        get
        {
            int count = 0;
            int index = (int)(uint)Volatile.Read(ref _idleHead) - 1;

            while (index >= 0 && count <= _slots.Length)
            {
                count++;
                index = Volatile.Read(ref _slots[index].Next);
            }

            return count;
        }
    }

    /// <summary>
    /// Test seam: workers currently registered in the timed wait (<c>TryTake</c>). Always zero in the
    /// thread-free build, which has no workers.
    /// </summary>
    internal int TimedWaiterCount =>
#if KOMMANDER_THREAD_FREE
        0;
#else
        Volatile.Read(ref _timedWaiterCount);
#endif

#if KOMMANDER_THREAD_FREE
    /// <summary>The thread-free build has no workers to stop.</summary>
    public bool StopRequested => false;

    /// <summary>The thread-free build has no workers to stop.</summary>
    public void RequestStop()
    {
    }
#else
    // ── Worker side ────────────────────────────────────────────────────────

    /// <summary>True once <see cref="RequestStop"/> ran.</summary>
    public bool StopRequested => Volatile.Read(ref _stopRequested) != 0;

    /// <summary>
    /// Tells every worker to leave: <see cref="Take"/> and <see cref="TryTake"/> return false from then
    /// on, whatever the queue holds, and every parked or waiting worker is woken to see it. What is
    /// still queued is the owner's to drain with <see cref="TryDequeue"/>.
    ///
    /// <para>The flag is published with a full fence before the idle stack is emptied, and a parking
    /// worker pushes itself before it reads the flag, so the two cannot both miss each other.</para>
    /// </summary>
    public void RequestStop()
    {
        Interlocked.Exchange(ref _stopRequested, 1);
        WakeAllIdle();

        lock (_timedLock)
        {
            foreach (WorkerSlot waiter in _timedWaiters)
                waiter.Event.Set();

            _timedWaiters.Clear();
            Volatile.Write(ref _timedWaiterCount, 0);
        }
    }

    /// <summary>
    /// Blocks the calling worker until it has an item or a stop was requested: take, else spin (if this
    /// worker can be the one spinner and the window is open), else park.
    /// </summary>
    /// <param name="worker">The caller's slot index, 0..workerCount-1. One thread per index.</param>
    /// <param name="handOn">
    /// True when the caller takes one item per call: after its take it wakes a parked worker for what
    /// is still queued (<see cref="HandOnIfQueued"/>). False when the caller goes on to sweep the
    /// queue with <see cref="TryDequeue"/>; it then calls <see cref="HandOnIfQueued"/> itself for
    /// whatever it left.
    /// </param>
    /// <param name="item">The item taken.</param>
    /// <returns>False when a stop was requested; the worker must then leave its loop.</returns>
    public bool Take(int worker, bool handOn, [MaybeNullWhen(false)] out T item)
    {
        WorkerSlot slot = _slots[worker];
        bool justWoken = false;

        while (true)
        {
            if (Volatile.Read(ref _stopRequested) != 0)
            {
                item = default;
                return false;
            }

            if (_queue.TryDequeue(out item))
            {
                if (handOn)
                    HandOnIfQueued();

                return true;
            }

            if (justWoken)
            {
                justWoken = false;
                Interlocked.Increment(ref _emptyWakeups);
            }

            if (TrySpinForWork())
                continue;

            long parkedAt = maxSpinTicks > 0 ? Stopwatch.GetTimestamp() : 0;

            if (Park(slot))
            {
                justWoken = true;
                Interlocked.Increment(ref _wakeups);

                if (maxSpinTicks > 0)
                    OpenSpinWindowAfterPark(Stopwatch.GetTimestamp() - parkedAt);
            }
        }
    }

    /// <summary>
    /// Waits up to <paramref name="millisecondsTimeout"/> for an item, for a worker that already holds
    /// work and wants more before it starts. One wait per call.
    ///
    /// <para><b>No lost wake.</b> The worker registers (an interlocked increment, a full fence) and
    /// then re-checks the queue; a producer publishes and then reads the count. One of the two sees
    /// the other. A producer that finds a timed waiter wakes it in place of a parked worker, so the
    /// caller must call <see cref="HandOnIfQueued"/> once it stops waiting: the item it was woken for
    /// may still be queued if it left at the same moment.</para>
    /// </summary>
    /// <param name="worker">The caller's slot index.</param>
    /// <param name="millisecondsTimeout">Longest wait.</param>
    /// <param name="item">The item taken.</param>
    /// <param name="timedOut">
    /// When the call returns false: true if the time ran out or a stop was requested, false if the
    /// worker was woken and another worker took the item first (the caller may wait again).
    /// </param>
    public bool TryTake(int worker, int millisecondsTimeout, [MaybeNullWhen(false)] out T item, out bool timedOut) =>
        TryTakeWhile<object?>(worker, millisecondsTimeout, null, static _ => true, out item, out timedOut);

    /// <summary>
    /// <see cref="TryTake"/> for a worker that waits for a condition of its
    /// owner as well as for an item: it sleeps only while <paramref name="keepWaiting"/> answers true.
    ///
    /// <para><b>No lost wake for the condition.</b> The worker registers (an interlocked increment, a
    /// full fence) and then evaluates <paramref name="keepWaiting"/>. The thread that changes the
    /// condition publishes the change with a full fence (an interlocked operation on what the
    /// predicate reads) and then calls <see cref="WakeTimedWaiter(int)"/>, which reads the count. One
    /// of the two sees the other: either the predicate already reads the change and the worker does
    /// not sleep, or the waker finds the worker registered and sets its event.</para>
    /// </summary>
    /// <param name="worker">The caller's slot index.</param>
    /// <param name="millisecondsTimeout">Longest wait.</param>
    /// <param name="state">Passed to <paramref name="keepWaiting"/>, so the predicate can be a static lambda.</param>
    /// <param name="keepWaiting">
    /// Evaluated once, after the worker registered and before it sleeps. False means the condition
    /// the caller waits for already changed; the call then returns without a sleep, and reports it as
    /// a wake (<paramref name="timedOut"/> false) when no item is queued.
    /// </param>
    /// <param name="item">The item taken.</param>
    /// <param name="timedOut">As in <see cref="TryTake"/>.</param>
    public bool TryTakeWhile<TState>(
        int worker,
        int millisecondsTimeout,
        TState state,
        Func<TState, bool> keepWaiting,
        [MaybeNullWhen(false)] out T item,
        out bool timedOut)
    {
        timedOut = false;

        if (_queue.TryDequeue(out item))
            return true;

        WorkerSlot slot = _slots[worker];
        slot.Event.Reset();

        lock (_timedLock)
        {
            _timedWaiters.Add(slot);
            Interlocked.Increment(ref _timedWaiterCount);
        }

        bool signaled = true;

        if (_queue.IsEmpty && Volatile.Read(ref _stopRequested) == 0 && keepWaiting(state))
            signaled = slot.Event.Wait(millisecondsTimeout);

        // A producer sets the event inside the lock, in the section that removes the slot. So once this
        // section finds the slot gone, the set has happened, and no late set can end a later wait.
        lock (_timedLock)
        {
            if (_timedWaiters.Remove(slot))
                Interlocked.Decrement(ref _timedWaiterCount);
        }

        if (Volatile.Read(ref _stopRequested) != 0)
        {
            timedOut = true;
            item = default;
            return false;
        }

        if (_queue.TryDequeue(out item))
            return true;

        timedOut = !signaled;
        return false;
    }

    private bool TryWakeTimedWaiter()
    {
        lock (_timedLock)
        {
            int last = _timedWaiters.Count - 1;

            if (last < 0)
                return false;

            WorkerSlot waiter = _timedWaiters[last];
            _timedWaiters.RemoveAt(last);
            Interlocked.Decrement(ref _timedWaiterCount);
            waiter.Event.Set();
            return true;
        }
    }

    /// <summary>
    /// Becomes the one spinner, if nobody is and the window is open, and polls the queue for up to the
    /// current window. True when an item (or a stop request) appeared; the caller loops to take it. A
    /// miss narrows the window.
    ///
    /// <para><b>No lost wake on the way out.</b> A producer that skipped its wake read
    /// <see cref="_spinning"/> as 1 after it published its item. The spinner clears the flag with a full
    /// fence before it parks, and <see cref="Park"/> re-checks the queue after that, so it sees every
    /// item whose producer counted on it.</para>
    /// </summary>
    private bool TrySpinForWork()
    {
        long window = Volatile.Read(ref _spinWindowTicks);

        if (window <= 0 || Interlocked.CompareExchange(ref _spinning, 1, 0) != 0)
            return false;

        bool found = false;
        try
        {
            long deadline = Stopwatch.GetTimestamp() + window;

            do
            {
                if (!_queue.IsEmpty || Volatile.Read(ref _stopRequested) != 0)
                {
                    found = true;
                    break;
                }

                Thread.SpinWait(4);
            }
            while (Stopwatch.GetTimestamp() < deadline);
        }
        finally
        {
            Interlocked.Exchange(ref _spinning, 0);
        }

        if (!found)
        {
            long narrowed = window / 2;
            Volatile.Write(ref _spinWindowTicks, narrowed < maxSpinTicks / 16 ? 0 : narrowed);
        }

        return found;
    }

    /// <summary>
    /// Widens the spin window when a park ended sooner than the bound: a spin of that length would have
    /// taken the item without the kernel wake.
    /// </summary>
    private void OpenSpinWindowAfterPark(long parkedTicks)
    {
        if (parkedTicks >= maxSpinTicks)
            return;

        long window = Volatile.Read(ref _spinWindowTicks);
        long widened = window <= 0 ? Math.Max(1, maxSpinTicks / 4) : Math.Min(maxSpinTicks, window * 2);
        Volatile.Write(ref _spinWindowTicks, widened);
    }

    private void PushIdle(WorkerSlot slot)
    {
        while (true)
        {
            long head = Volatile.Read(ref _idleHead);
            slot.Next = (int)(uint)head - 1;

            if (Interlocked.CompareExchange(ref _idleHead, PackIdleHead(head, slot.Index), head) == head)
                return;
        }
    }

    /// <summary>
    /// Parks the worker until a producer pops its slot and sets its event.
    ///
    /// <para><b>No lost wake.</b> A producer publishes an item (a <see cref="ConcurrentQueue{T}"/>
    /// enqueue, whose tail claim is an interlocked operation) and then pops the idle stack. The worker
    /// pushes itself on the idle stack (a compare-exchange) and then re-checks the queue. Interlocked
    /// operations are full fences, so the two store-then-load sequences cannot both miss: either the
    /// producer's pop finds the worker, or the worker's re-check finds the item. When the re-check finds
    /// an item, the worker wakes one idle worker for it, possibly itself, in which case it returns
    /// without sleeping; if the stack is already empty, someone popped this worker and will set its
    /// event, so it waits for that set. The same argument covers <see cref="RequestStop"/>.</para>
    ///
    /// <para>An item from <see cref="EnqueueFromWorker"/> does not depend on this worker: its producer
    /// is on its way back to the queue.</para>
    /// </summary>
    /// <returns><c>true</c> when the worker slept and was woken by another thread.</returns>
    private bool Park(WorkerSlot slot)
    {
        slot.Event.Reset();
        PushIdle(slot);

        if (Volatile.Read(ref _stopRequested) != 0)
        {
            // RequestStop may have emptied the stack before this push: wake everyone still on it,
            // this worker included, and go back to the loop, which sees the flag and leaves.
            WakeAllIdle();
            return false;
        }

        if (!_queue.IsEmpty && TryPopIdle(out WorkerSlot? other))
        {
            if (ReferenceEquals(other, slot))
                return false;

            other!.Event.Set();
        }

        slot.Event.Wait();
        return true;
    }
#endif

    public void Dispose()
    {
        foreach (WorkerSlot slot in _slots)
            slot.Dispose();
    }
}
