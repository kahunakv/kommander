using Kommander.Consensus;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// A bounded exhaustive model of the Raft §5.4.1 log-freshness comparison (fragility analysis,
/// recommendation 4).
///
/// <para><b>Why exhaustive and not by example.</b> A scripted election test samples one schedule
/// and therefore one pair of log positions. The comparison itself is a total function over two
/// (term, index) pairs, so a small bound covers it completely: every case in the space is checked,
/// not a chosen few. That is the difference the fragility analysis draws between finding a bug
/// instance and closing a bug class — for this one function the class can be closed outright.</para>
///
/// <para><b>The bound.</b> Terms 0..4 and indices -1..4, both sides: 900 ordered pairs. The
/// interesting boundaries are all inside it — term 0 (the legacy and empty-log path), index -1
/// (an empty log), and equal terms with unequal indices. Values above the bound add arithmetic,
/// not cases, because the function only compares.</para>
///
/// <para><b>What is asserted.</b> The relation "the candidate is behind" must behave like a strict
/// order derived from the (term, index) ranking:</para>
///
/// <list type="number">
///   <item>Irreflexive: an identical log is never behind, so a node always grants its vote to a
///         peer whose log matches its own.</item>
///   <item>Asymmetric: two nodes never each judge the other behind, which would leave a term with
///         no possible winner.</item>
///   <item>Transitive over real terms, so a three-way election has a stable ranking.</item>
///   <item>Safe: a strictly fresher candidate is never judged behind.</item>
///   <item>Faithful to the §5.4.1 ranking: term dominates index.</item>
///   <item>The legacy zero-term path compares indices only, on that side alone.</item>
/// </list>
/// </summary>
public sealed class TestElectionFreshnessModel
{
    private const int MaxTerm = 4;
    private const int MinIndex = -1;
    private const int MaxIndex = 4;

    private static IEnumerable<(long Term, long Index)> Positions()
    {
        for (long term = 0; term <= MaxTerm; term++)
        {
            for (long index = MinIndex; index <= MaxIndex; index++)
                yield return (term, index);
        }
    }

    private static bool IsBehind((long Term, long Index) remote, (long Term, long Index) local) =>
        ElectionFreshness.CandidateLogIsBehind(remote.Term, remote.Index, local.Term, local.Index);

    [Fact]
    public void AnIdenticalLogIsNeverBehind()
    {
        foreach ((long Term, long Index) position in Positions())
        {
            Assert.False(
                IsBehind(position, position),
                $"a log identical to the voter's own was judged behind: {position}");
        }
    }

    [Fact]
    public void TwoNodesNeverJudgeEachOtherBehind()
    {
        foreach ((long Term, long Index) a in Positions())
        {
            foreach ((long Term, long Index) b in Positions())
            {
                bool aBehindB = IsBehind(a, b);
                bool bBehindA = IsBehind(b, a);

                Assert.False(
                    aBehindB && bBehindA,
                    $"both {a} and {b} were judged behind the other; no candidate could win the term");
            }
        }
    }

    [Fact]
    public void AStrictlyFresherLogIsNeverJudgedBehind()
    {
        // The safety half of §5.4.1: a candidate that holds everything the voter holds, and more,
        // must never be denied. A denial here is how a partition loses its only complete replica
        // as a leadership candidate.
        foreach ((long Term, long Index) remote in PositiveTermPositions())
        {
            foreach ((long Term, long Index) local in PositiveTermPositions())
            {
                bool remoteIsFresher =
                    remote.Term > local.Term
                    || (remote.Term == local.Term && remote.Index > local.Index);

                if (remoteIsFresher)
                    Assert.False(IsBehind(remote, local), $"{remote} is fresher than {local} yet was judged behind");
            }
        }
    }

    [Fact]
    public void TheRelationIsTransitive()
    {
        // Over real terms the rule must be a strict order. An intransitive comparison would let a
        // three-way election cycle through candidates with no stable winner.
        (long Term, long Index)[] space = PositiveTermPositions().ToArray();

        foreach ((long Term, long Index) a in space)
        {
            foreach ((long Term, long Index) b in space)
            {
                if (!IsBehind(a, b))
                    continue;

                foreach ((long Term, long Index) c in space)
                {
                    if (IsBehind(b, c))
                        Assert.True(IsBehind(a, c), $"{a} < {b} and {b} < {c} but {a} was not behind {c}");
                }
            }
        }
    }

    [Fact]
    public void TermDominatesIndex()
    {
        foreach ((long Term, long Index) remote in Positions())
        {
            foreach ((long Term, long Index) local in Positions())
            {
                if (remote.Term <= 0)
                    continue; // the legacy path has its own test below

                if (remote.Term == local.Term)
                    continue;

                Assert.Equal(remote.Term < local.Term, IsBehind(remote, local));
            }
        }
    }

    [Fact]
    public void AtAnEqualTermTheHigherIndexWins()
    {
        foreach ((long Term, long Index) remote in Positions())
        {
            if (remote.Term <= 0)
                continue;

            for (long index = MinIndex; index <= MaxIndex; index++)
                Assert.Equal(remote.Index < index, IsBehind(remote, (remote.Term, index)));
        }
    }

    [Fact]
    public void AZeroRemoteTermFallsBackToIndexOnly()
    {
        // A peer that sends no term is a legacy sender or a candidate with an empty log. The local
        // term must not enter the comparison at all on that path, whatever it holds.
        for (long remoteIndex = MinIndex; remoteIndex <= MaxIndex; remoteIndex++)
        {
            foreach ((long Term, long Index) local in Positions())
            {
                Assert.Equal(
                    remoteIndex < local.Index,
                    IsBehind((0, remoteIndex), local));
            }
        }
    }

    [Fact]
    public void ANegativeRemoteTermTakesTheSameLegacyPath()
    {
        // GetFreshnessLogPositionAsync can return a negative term from a stub facade; the rule must
        // treat "no term" and "negative term" alike rather than ranking below every real term.
        foreach ((long Term, long Index) local in Positions())
        {
            Assert.Equal(
                IsBehind((0, 2), local),
                IsBehind((-3, 2), local));
        }
    }

    /// <summary>
    /// The subset of the space with a real term. The zero and negative terms take the legacy
    /// index-only path, which is deliberately not a strict order against a real-term peer — two
    /// such nodes may each grant the other a vote, and Raft's term restriction, not this
    /// comparison, is what keeps that safe.
    /// </summary>
    private static IEnumerable<(long Term, long Index)> PositiveTermPositions() =>
        Positions().Where(p => p.Term > 0);
}
