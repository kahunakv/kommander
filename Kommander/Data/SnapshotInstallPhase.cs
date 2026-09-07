using System.ComponentModel;

namespace Kommander.Data;

/// <summary>
/// The point in the follower-side snapshot install at which
/// <c>IRaft.SetSnapshotInstallGateForTesting</c> suspends the operation.
///
/// <para>The install runs a fixed, recoverable order: validate and adopt the leader, import into
/// the registered consumer, write the durable WAL boundary (which retains the suffix above the
/// index on a matching term and truncates it on conflict), then reconstruct the apply cursor and
/// the commit frontier. A gate runs strictly <b>between</b> those steps and never reorders
/// them.</para>
///
/// <para>The idempotent re-install short-circuit returns before any of these steps, so no phase
/// fires for it.</para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum SnapshotInstallPhase
{
    /// <summary>
    /// After leader validation and adoption, before the consumer import. The consumer still holds
    /// its pre-install state.
    /// </summary>
    BeforeImport,

    /// <summary>
    /// After the consumer import succeeded, before the durable WAL boundary. A crash here is
    /// recoverable: the import is idempotent and the boundary is not yet durable, so the sender
    /// retries the whole snapshot.
    /// </summary>
    AfterImport,

    /// <summary>
    /// After the durable WAL boundary was installed — the retain-or-truncate decision on the log
    /// tail has been made — and before the apply cursor and commit frontier are seeded.
    /// </summary>
    AfterBoundary,
}
