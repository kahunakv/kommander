using Kommander.Data;

namespace Kommander.Tests.Simulation.History;

/// <summary>
/// What the cluster told a client about one operation.
///
/// <para>Three outcomes, not two, and the third is the one that matters. A distributed system
/// answers "yes", "no", and "I do not know", and a history checker that collapses the third into
/// either of the others is wrong in one direction or the other: it will either accept a lost write
/// or report a phantom one.</para>
/// </summary>
public enum ClientOperationOutcome
{
    /// <summary>The cluster acknowledged the operation. It must be visible afterwards.</summary>
    Ok = 0,

    /// <summary>
    /// The cluster refused the operation before it could take effect. It must <b>not</b> be visible
    /// afterwards. Only statuses that cannot have reached the log are classified here.
    /// </summary>
    Fail = 1,

    /// <summary>
    /// The outcome is unknown. The operation may or may not have taken effect, and both are legal.
    /// A timeout, a cancellation, and a replication failure all land here: the entry may already be
    /// on a quorum with only the acknowledgement lost.
    /// </summary>
    Info = 2,
}

/// <summary>
/// One recorded client operation, from the moment it was issued to the answer it received.
/// </summary>
/// <param name="Id">Position in the history, unique within a run.</param>
/// <param name="Type">The log type the client asked for.</param>
/// <param name="Payload">
/// Exactly the bytes the client sent. The checker matches on these rather than on a hash: the
/// histories are short, and a hash would add a collision caveat to every verdict for no gain.
/// </param>
/// <param name="InvokedAtStep">Simulation step at which the call was made, for the report.</param>
/// <param name="CompletedAtStep">Simulation step at which the answer arrived, for the report.</param>
/// <param name="InvokedAtSequence">
/// Position in the history's own real-time order when the call was made.
///
/// <para>The step number cannot serve here. A step is coarse: a client can issue and complete
/// several appends inside one step, and comparing step numbers would then call them concurrent and
/// decline to order operations the client itself observed one after another. The sequence advances
/// on every invocation and every completion, so two calls a client made in turn are always strictly
/// ordered while two genuinely in flight together interleave.</para>
/// </param>
/// <param name="CompletedAtSequence">Position in that order when the answer arrived.</param>
/// <param name="Outcome">How the answer is classified.</param>
/// <param name="Status">The raw status, kept so a failure report can name it.</param>
/// <param name="LogIndex">The index the cluster assigned, meaningful only when the outcome is Ok.</param>
public sealed record ClientOperation(
    int Id,
    string Type,
    byte[] Payload,
    int InvokedAtStep,
    int CompletedAtStep,
    int InvokedAtSequence,
    int CompletedAtSequence,
    ClientOperationOutcome Outcome,
    RaftOperationStatus Status,
    long LogIndex)
{
    /// <summary>A short description for a failure message.</summary>
    public override string ToString() =>
        $"op{Id}({Type}) {Outcome}/{Status} index={LogIndex} steps={InvokedAtStep}..{CompletedAtStep}";
}
