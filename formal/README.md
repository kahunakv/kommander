# Formal models

This folder holds TLA+ models of Kommander and the scripts that check them. A model states how a
part of the protocol must behave. The TLC model checker then explores every reachable state of a
small configuration and reports the first state that breaks a stated property.

The approach follows "lightweight formal methods" as the Amazon S3 team describes it: keep the
model next to the code, check it on every change, and connect it to the real code so that the two
cannot drift apart. This folder does both halves:

1. **Model checking** (`tla/KommanderDurability.tla`): the commit and durability path of one
   partition.
2. **Trace validation** (`tla/WalDurabilityTrace.tla`): the deterministic simulation records what
   the real code did, and TLC replays it through the same model operators. Every frontier value the
   code computed must equal the value the model computes.

## Run it

You need Java 11 or later. On macOS: `brew install openjdk@21`.

```sh
formal/run-tlc.sh                    # every config and every stored trace, checked against its EXPECT line
formal/run-tlc.sh Current            # one config
formal/run-tlc.sh --trace <dir|file> # validate traces recorded by a DST run (must pass)
```

The script downloads `tla2tools.jar` (pinned by version and SHA-256) into `formal/.tools/`, which
git ignores. Logs and counterexamples go to `formal/.tools/logs/`. TLC uses 2 worker threads by
default, so a run does not take every core from other work. Set `TLC_WORKERS` to change it.

A full run took 3 minutes 15 seconds on 2 workers (Apple silicon, 8 cores). Most of that is the two
exhaustive passing configs.

## What the model covers

`tla/WalDurability.tla` is the write-ahead state of one replica, as pure operators over a record.
Every field mirrors a field of `Kommander/RaftWriteAhead.cs`, with the same exclusive convention:
`commitIndex`, `pendingResolved`, `presentIndex`, `pendingPresent`, `durablePresentIndex`,
`pendingDurable`, `durableResolvedIndex`, `ridingResolvedIndex`, `ridingResolvedFloor`,
`writtenResolvedIndex`, `pendingWrittenResolved`. It also models:

- the per-partition FIFO queue of `FairWalScheduler` and its single-fsync rule: a batch skips its
  own fsync only when every row is a Committed marker and no operation is flagged `RequiresSync`;
- a disk with a durable part and a sync-off part. A synced write carries every sync-off row before
  it. A crash loses the sync-off part and the queue;
- restore (`CompleteRestoreAsync`), which re-derives every frontier from the disk;
- proposed-tail truncation (`TruncateProposedLogsAfter`).

`tla/KommanderDurability.tla` adds one stable leader and its followers: unanchored proposes and
commit markers, commit on a durable quorum, the retention floor from each follower's reported
durable frontier, compaction, and anchored backfill with the `-1` anchor rule. The network loses,
duplicates and reorders appends.

### Properties

| Property | Meaning |
| --- | --- |
| `ReportIsDurableAll` | What a follower reports as durable survives a crash right now. |
| `StaleReportsAreDurable` | No report the leader ever saw is above what the follower can restore now. |
| `ViewIsDurable` | The leader's record of each follower is never above what it can restore. |
| `CompactionServable` | Compaction never removes a row that a follower needs after a crash. |
| `CommittedOnQuorum` | A committed entry is durable on a quorum. |
| `DurablePresenceIsDurableAll` | `durablePresentIndex` certifies only rows that are on disk. |
| `BackfillAnchorAccepted` | When the leader's record is current, its next backfill is accepted. |
| `NoCommittedRowRegression` | A Committed row on disk never turns back into a Proposed row (hygiene). |

### Not covered yet (slice 1)

- Leader change: elections, the promotion barrier, inherited re-commit, the propose allocator.
- Terms: divergent tails, the term-inversion guard.
- Write failures and the frontier regression after them.
- Follower compaction, snapshot install, checkpoints, membership.
- Liveness ("the cluster eventually converges").
- Batches with more than one operation (the model writes one operation per batch).

## The configs

Each config starts with `\* EXPECT: ...`. The run fails when a verdict differs from it, in either
direction.

| Config | Report rule | Expected | What it proves |
| --- | --- | --- | --- |
| `Current` | the code today | pass | Every property holds: 1 follower, 3 entries, 1 crash, 2 queued writes. |
| `CurrentTwoFollowers` | the code today | pass | The same with 2 followers and 2 entries, which reaches the first-write paths. |
| `ControlFinding6` | `current`, `AnchorFix = FALSE` | `BackfillAnchorAccepted` violated | The model finds FINDING 6. |
| `ControlFinding7` | `inMemoryCommit` | `ReportIsDurableAll` violated | The model finds FINDING 7. |
| `ControlFinding8` | `noRidingFloor` | `ReportIsDurableAll` violated | The model finds FINDING 8. |
| `ControlFinding9` | `preFinding9` | `ReportIsDurableAll` violated | The model finds FINDING 9. |
| `ControlFirstWrite` | `current`, `FirstWriteFix = FALSE` | `DurablePresenceIsDurableAll` violated | The model finds the committed-first-write defect. |
| `Hygiene` | `current` | `NoCommittedRowRegression` violated | FINDING 10 (below), still open. |

**Why the controls exist.** A control turns one fixed defect back on. If a control passes, the
model cannot see that defect, and a passing config proves nothing about it. Each control switches
back on exactly one defect over the fixed baseline, so its verdict is about that defect alone.

**Why two passing configs.** With one follower, the leader commits an entry only after that
follower holds it, so a Committed row can never be a follower's first write of that row. The
two-follower config reaches that path. It uses 2 entries to stay at about 4 minutes.

## Findings

**FINDING 9 (fixed): the reported durable frontier counted a resolution that was still
queued.** The commit frontier advances when a resolution is enqueued. If a synced resolution of a
higher id completed first, `durableResolvedIndex` moved past the lower id while its marker was still
in the WAL queue (or written sync-off, with its completion not yet processed). The FINDING 8 floor
covers only a marker whose completion was processed. The model reached it in 13 steps, and the
deterministic simulation reached it on the first traced seed (event 49 of
`tla/traces/Finding9Seed20261022Node1.tla`, recorded before the fix). The fix adds
`writtenResolvedIndex`: a resolution frontier that advances when a completion is processed,
gap-buffered over the exact resolved ids the completion carries (`ResolvedSparseLogIds`). It bounds
the reported frontier. `ControlFinding9` keeps the old rule and must still fail.

**FINDING 10 (hygiene): a restart unprotects a Committed row above a hole.** Restore does not put
Committed rows above the first hole into `pendingResolved`. A late Proposed copy of such a row then
passes the resolved-id guard and overwrites the Committed row on disk. No durability property
breaks, because the follower never reported that row. But the rule in `EnqueueProposeOrCommit`
("a locally resolved id must never be written as Proposed again") does not hold after a restart.

## Trace validation

The DST harness records each node's write-ahead events when
`SimulationClusterOptions.RecordWalTrace` is set. `RaftWriteAhead` and `FairWalScheduler` emit the
events through the internal `RaftConfiguration.WalTraceSink`, which is null in production (one null
check per emission point). `WalTraceRecorder` writes one TLA+ module per node and partition.

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "FullyQualifiedName~TestWalTraceRecording"
formal/run-tlc.sh --trace Kommander.Tests/bin/Debug/net10.0/dst-wal-traces/20261022
```

Set `KOMMANDER_DST_WAL_TRACE_SEED` for another seed and `KOMMANDER_DST_WAL_TRACE_DIR` for another
output directory.

What the validator checks at each event:

- `durablePresentIndex`, `durableResolvedIndex`, `ridingResolvedIndex`, `ridingResolvedFloor` and
  the reported durable frontier equal the model's values;
- at a restore, the commit and presence frontiers equal what the model restores from its disk;
- the reported frontier never exceeds what a restart would restore (`TraceReportIsDurable`).

`commitIndex` and `presentIndex` are inputs, because they move on paths slice 1 does not model. The
replay stops at the first event the model does not cover (a snapshot install, a compaction, a
restore seeded from a floor, a crash on a store that breaks the fsync contract on purpose), and the
summary line says how far it got.

`tla/traces/` holds three stored traces that every default run checks:

- `PassingSeed20261022Node1`: recorded after the FINDING 9 fix; every event must match.
- `ControlChangedWrittenResolved`: the same trace with one `writtenResolvedIndex` value raised by
  one; the validator must reject it.
- `Finding9Seed20261022Node1`: recorded before the fix; the old code's report differs from the
  model at event 49, so the validator must reject it.
