
using Kommander.Data;

namespace Kommander.Tests.Chaos;

/// <summary>
/// The logical Raft verb a transport message carries, normalized across the 14 <c>ICommunication</c>
/// methods AND the individual <c>BatchRequests</c> item types, so a nemesis rule can select a message by
/// what it MEANS ("drop every TransferLeadership from a to b") rather than which outer transport method
/// happened to carry it.
/// </summary>
public enum NemesisVerb
{
    Handshake,
    RequestVotes,
    Vote,
    AppendLogs,
    CompleteAppendLogs,
    StepDownNotice,
    TransferLeadership,
    TransferLeadershipSuggestion,
    Join,
    Leave,
    Gossip,
    Ping,
    PingReq,
    GetFollowerLag,
    InstallSnapshot,
    NotifyJoinBlocked,
}

/// <summary>
/// What the nemesis does to a single message. Actions are per directed link and may additionally select
/// verb and partition.
/// </summary>
public enum FaultAction
{
    /// <summary>Deliver unchanged.</summary>
    Pass,

    /// <summary>Do not deliver; complete with the transport's neutral/unreachable response shape.</summary>
    Drop,

    /// <summary>Complete with a configured transport exception.</summary>
    Fail,

    /// <summary>Deliver after a bounded delay.</summary>
    Delay,

    /// <summary>Retain until an explicit release.</summary>
    Hold,

    /// <summary>Deliver two cloned copies, in a defined order (original first, then a deep copy).</summary>
    Duplicate,
}

/// <summary>
/// A normalized, immutable description of one outbound logical message. Deliberately holds only value
/// attributes (endpoints, verb, partition, a correlation string) — never object identity or a mutable
/// request reference — so it is stable to select on and safe to retain in the event log.
/// </summary>
public sealed record NemesisEnvelope(
    long Sequence,
    string Source,
    string Destination,
    NemesisVerb Verb,
    int? Partition,
    string? Correlation)
{
    public override string ToString() =>
        $"#{Sequence} {Source}->{Destination} {Verb}" +
        (Partition is int p ? $" p{p}" : "") +
        (Correlation is { Length: > 0 } c ? $" [{c}]" : "");
}

/// <summary>What kind of thing the event records — a decision, a delivery, or a lifecycle transition.</summary>
public enum NemesisEventKind
{
    Decision,
    Delivery,
    Release,
    Cancellation,
    Drop,
    Fail,
    Delay,
    Duplicate,
    Hold,
}

/// <summary>
/// One immutable entry in the nemesis event log. Records decisions, releases, cancellations, and
/// deliveries — not only injected faults — so a failure report can show exactly what the transport did.
/// </summary>
public sealed record NemesisEvent(NemesisEventKind Kind, NemesisEnvelope Envelope, string? Note = null)
{
    public override string ToString() =>
        $"{Kind,-12} {Envelope}" + (Note is { Length: > 0 } n ? $" ({n})" : "");
}

/// <summary>
/// Weights for the randomized fault profile: when no scripted rule applies, one action is drawn per
/// envelope from the seeded PRNG in observed-sequence order, so a fixed envelope order + seed yields the
/// same decisions. Weights need not sum to 1; the remaining probability mass is <see cref="FaultAction.Pass"/>.
/// </summary>
public sealed class NemesisRandomProfile
{
    public double Drop { get; init; }
    public double Fail { get; init; }
    public double Delay { get; init; }
    public double Duplicate { get; init; }

    /// <summary>Delay applied when the random profile selects <see cref="FaultAction.Delay"/>.</summary>
    public TimeSpan DelayDuration { get; init; } = TimeSpan.FromMilliseconds(20);

    /// <summary>Exception used when the random profile selects <see cref="FaultAction.Fail"/>.</summary>
    public Func<Exception> ExceptionFactory { get; init; } = static () => new NemesisTransportException();

    internal FaultAction Roll(Random rng)
    {
        double r = rng.NextDouble();
        double t = Drop;
        if (r < t) return FaultAction.Drop;
        t += Fail;
        if (r < t) return FaultAction.Fail;
        t += Delay;
        if (r < t) return FaultAction.Delay;
        t += Duplicate;
        if (r < t) return FaultAction.Duplicate;
        return FaultAction.Pass;
    }
}

/// <summary>The default transport exception the nemesis raises for <see cref="FaultAction.Fail"/>.</summary>
public sealed class NemesisTransportException : Exception
{
    public NemesisTransportException() : base("Nemesis-injected transport failure.") { }
    public NemesisTransportException(string message) : base(message) { }
}

/// <summary>
/// A scripted fault rule: a selector over stable envelope attributes plus an optional 1-based occurrence,
/// and the action to apply. Rules are evaluated in insertion order; the first matching, occurrence-eligible
/// rule wins. A named rule can be removed as a group via <c>Heal</c> (used by partition sugar).
/// </summary>
public sealed class NemesisRule
{
    public string? Source { get; init; }
    public string? Destination { get; init; }
    public NemesisVerb? Verb { get; init; }
    public int? Partition { get; init; }

    /// <summary>When set, the rule applies only to the Nth (1-based) envelope that matches its selectors.</summary>
    public int? Occurrence { get; init; }

    public FaultAction Action { get; init; }
    public TimeSpan Delay { get; init; }
    public Func<Exception>? ExceptionFactory { get; init; }

    /// <summary>Group name for partition/heal sugar; independent rules can be left unnamed.</summary>
    public string? Name { get; init; }

    // Mutable match counter, only touched under the nemesis lock.
    internal int MatchCount;

    internal bool SelectorsMatch(NemesisEnvelope env) =>
        (Source is null || Source == env.Source)
        && (Destination is null || Destination == env.Destination)
        && (Verb is null || Verb == env.Verb)
        && (Partition is null || Partition == env.Partition);
}
