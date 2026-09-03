# Deterministic simulation testing

A seeded search for states that break Raft, run against real three-node clusters in one process.
This page is for whoever has to act on a failure.

## The three categories

| Category | Where it runs | What it costs |
| --- | --- | --- |
| `DSTSmoke` | every push and pull request | seconds; one short generated run |
| `DSTRandom` | the nightly job | minutes, and grows with the seed count |
| `DSTProbe` | by hand, skipped otherwise | whatever you give it |

`DSTRandom` is excluded from the pull-request job on purpose. Its cost grows with the seed count, and
a seed that fails is a lead to investigate rather than a verdict on the change under review.

## Running it

```sh
# Everything a pull request runs.
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category=DSTSmoke"

# The search, at the local default of two swept seeds beside the corpus.
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category=DSTRandom"

# A longer search, from a different region.
KOMMANDER_DST_SEED_BASE=20270101 KOMMANDER_DST_SEED_COUNT=64 \
  dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category=DSTRandom"
```

Never run two test commands at once. These tests start real clusters and timing-sensitive state
machines; concurrent runs interfere and produce failures that are about the machine.

## Reproducing a failure the nightly found

Download `dst-artifacts` from the failed run. It holds up to three files per failing seed: the plan
in the order it happened, the entropy the run consumed, and — because the nightly shrinks what it
finds — a reduced plan.

Then one command:

```sh
KOMMANDER_DST_REPLAY_DIR=/path/to/dst-artifacts KOMMANDER_DST_REPLAY_REPEATS=10 \
  dotnet test Kommander.Tests/Kommander.Tests.csproj \
  --filter "FullyQualifiedName~EveryPromotedPlan_StillHoldsEveryCheck"
```

**Replay the plan, not the seed.** A seed fixes the generator's draws, and a draw depends on what the
generator observed in a cluster whose nodes own their own threads. Two runs of one seed can observe
different leaders and diverge from there. The plan removes the generator: the same actions in the
same order, whatever the cluster does between them.

This distinction decides findings. Two failures once looked like load because neither *seed* failed
when re-run alone. Replaying one of the captured *plans* failed three times in ten, with identical
frontiers — the state was real, and the seed had been the wrong unit.

**Read the rate, not the fact.** The replay reports "failed 3 of 10 replays". One in ten and ten in
ten are different findings: the first is a rare state to hunt, the second is a defect to fix.

## Shrinking a plan

A plan of twenty-five actions has two or three that matter. The shrinker runs the re-runs a person
would run by hand.

```sh
KOMMANDER_DST_SHRINK_PLAN=/path/to/random-seed-N.plan.txt \
KOMMANDER_DST_SHRINK_BUDGET=700 \
  dotnet test Kommander.Tests/Kommander.Tests.csproj \
  --filter "FullyQualifiedName~AConfiguredPlan_ShrinksToItsCause"
```

**The probe measures the reproduction rate itself.** It runs the whole plan twenty times first,
counts the failures, and derives an attempt count that catches such a plan nine times in ten. That removes
the worst guess in a shrink: at one attempt per candidate, a plan that fails three times in ten reads
as passing on most candidates, and the shrinker then keeps almost every action and reports a
reduction of nothing — indistinguishable from a plan whose every action matters. Override the derived
count with `KOMMANDER_DST_SHRINK_ATTEMPTS`, and raise `KOMMANDER_DST_SHRINK_PROBE_RUNS` for a plan
rarer than that: if the probe reports the plan held every check, it did not reproduce in the runs it
was given, and the answer is more runs rather than a different plan.

**Read the catch probability the probe prints.** A shrink that removed nothing is only evidence that
every action is needed when that figure is high.

**The cost gets worse as the shrink succeeds.** A shorter plan usually reproduces less often than the
plan it came from, so the attempts needed rise exactly as the search makes progress. A shrink that
stops is worth re-running on its own output. The budget counts cluster runs, not candidates: seven
hundred runs is roughly half an hour for a short plan.

The probe writes the reduced plan beside the file you gave it, as `<name>.shrunk.plan.txt`, and
prints what the shrink cost. That file carries the seed and the bounds but **not** the original run's
step count or measurements — a plan of three actions has nothing to do with the four hundred steps
the plan it came from took.

The nightly does this by itself. `KOMMANDER_DST_SHRINK=1` makes a failing run reduce its own plan and
write a second artifact.

## Promoting a failure

A plan worth keeping goes in `Scenarios/Random/regressions/`, which has its own README. Copy the
file, rename it to say what it is, and it is replayed from then on.

Promote only a plan whose cause is understood. A plan that fails three times in ten is a coin toss in
the standing test set.

## Choosing seeds and bounds

The corpus seeds in `Scenarios/Random/seed-corpus.txt` always run, and each one earned its line by
finding something; the reason is written beside it. The sweep beside them moves with
`KOMMANDER_DST_SEED_BASE` and `KOMMANDER_DST_SEED_COUNT`.

Bounds live in `RandomScenarioOptions`, and each field says why its default is what it is. Two are
worth knowing about:

- `CompactEveryOperations` is the production default, so a generated run never compacts. A run that
  wants compaction lowers it. At eight, roughly one run in eight ends wedged — recorded as a finding
  rather than absorbed.
- `MaxImpairedNodes` is one. Two impaired nodes in a three-node cluster lose quorum, and a leader
  without quorum waits ten **real** seconds inside its quorum wait. A search that reaches that state
  stops exploring and starts paying.

A plan artifact's header records every bound, and a replay rebuilds them from the file. A plan
replayed at three steps per action is not the plan that failed at six.

## Metrics and the budget

Every generated run measures itself: steps, wall time, invariant time, managed memory, steps per
second, and the share of the run spent checking. The smoke run prints the line on every run.

`KOMMANDER_DST_BUDGET=ci` enforces limits, and both jobs set it. The limits are **stall detectors,
not speed limits**: a run below one step per ten seconds has stopped rather than slowed. The step
floor was five per second on the first attempt and would have failed a correct run on a loaded
machine — the one measurement that mattered was the worst correct run, not the typical one.

## The rule that saves the most time

**Run the control.** This suite is load-sensitive: the same category has taken 1 h 01 m and 2 m 08 s
on one machine, and both runs were correct. A single slow or failing run is never evidence. Re-run
with the change excluded, or replay the plan; either costs minutes and has inverted the conclusion
more than once.
