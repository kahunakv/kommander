namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// One decision a random run made, in the form a reader can act on.
///
/// <para>The record is the replay artifact. A generated run is reproducible from its seed, but a
/// seed says nothing to a reader looking at a failure; the action list says what happened. Keeping
/// both means a failure can be re-run by seed and understood by list.</para>
/// </summary>
/// <param name="Index">Position in the plan, counted from zero.</param>
/// <param name="Kind">What the action does.</param>
/// <param name="Target">Endpoint the action applies to, when it applies to one.</param>
/// <param name="Secondary">The far endpoint of a link action.</param>
/// <param name="Value">Numeric parameter: copies, write count, or milliseconds.</param>
public sealed record RandomScenarioAction(
    int Index,
    RandomScenarioActionKind Kind,
    string? Target = null,
    string? Secondary = null,
    long Value = 0)
{
    /// <summary>One line naming the action, for the plan artifact and the failure message.</summary>
    public string Describe()
    {
        string detail = Kind switch
        {
            RandomScenarioActionKind.BlockLink or RandomScenarioActionKind.UnblockLink =>
                $" {Target} -> {Secondary}",
            RandomScenarioActionKind.DuplicateLink =>
                $" {Target} -> {Secondary} copies={Value}",
            RandomScenarioActionKind.FailWrites =>
                $" {Target} writes={Value}",
            RandomScenarioActionKind.SlowDisk =>
                $" {Target} latencyMs={Value}",
            _ => Target is null ? string.Empty : $" {Target}",
        };

        return $"{Index:D3} {Kind}{detail}";
    }
}
