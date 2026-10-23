---------------------------- MODULE WalDurability ----------------------------
(***************************************************************************)
(* The write-ahead state of ONE Kommander partition replica, as pure       *)
(* operators over a node record.                                           *)
(*                                                                         *)
(* Two specs use these operators: the cluster model (KommanderDurability)  *)
(* and the trace validator (WalDurabilityTrace), which replays a DST       *)
(* node's events through them. One definition for both is the point: the   *)
(* trace check is only worth something if it tests the operators the model *)
(* checker explored.                                                       *)
(*                                                                         *)
(* Every field mirrors a field of Kommander/RaftWriteAhead.cs and uses the *)
(* same EXCLUSIVE convention ("next id not yet covered", initial value 1): *)
(*                                                                         *)
(*   commit      commitIndex           resolved ids, advanced at ENQUEUE   *)
(*   pendRes     pendingResolved       resolved ids above a gap            *)
(*   present     presentIndex          held ids, advanced at ENQUEUE       *)
(*   pendPres    pendingPresent        held ids above a gap                *)
(*   durPresent  durablePresentIndex   held ids, advanced at COMPLETION    *)
(*   pendDur     pendingDurable        durable ids above a gap             *)
(*   durRes      durableResolvedIndex  synced resolution high-water        *)
(*   ridingRes   ridingResolvedIndex   written resolution high-water       *)
(*   ridingFloor ridingResolvedFloor   lowest sync-off resolution waiting  *)
(*                                     for a carrier (Inf = long.MaxValue) *)
(*   resWritten  writtenResolvedIndex  resolved ids, advanced at           *)
(*   pendResW    pendingWrittenResolved COMPLETION, gap-buffered           *)
(*                                                                         *)
(* The disk has two parts. `disk` is durable. `pend` holds rows written    *)
(* sync-off that no later synced write on the partition has carried yet;   *)
(* a crash loses them, exactly the single-fsync contract that              *)
(* Kommander.Tests/Simulation/WAL/SimulatedWAL.cs implements. `queue` is   *)
(* the partition's FIFO in FairWalScheduler: accepted, not yet written.    *)
(*                                                                         *)
(* Abstractions (slice 1): one term, so log rows carry no term and the     *)
(* term-inversion guard never fires; no write failures, so                 *)
(* RegressFrontiersAfterFailedWriteAsync is not modelled; no follower      *)
(* compaction or snapshot install; one op per scheduler batch.             *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    N,              \* highest log index in the model
    LazyMarkers,    \* RaftConfiguration.WalSingleFsyncCommit
    FirstWriteFix,  \* TRUE: WALWriteOperation.RequiresSync for a never-held Committed row
    ReportRule      \* "current": the code today
                    \* "preFinding9": the rule before FINDING 9 (no written resolution frontier)
                    \* "noRidingFloor": the code today without the FINDING 8 floor
                    \* "inMemoryCommit": the rule before FINDING 7

ASSUME N \in Nat \ {0}
ASSUME LazyMarkers \in BOOLEAN /\ FirstWriteFix \in BOOLEAN
ASSUME ReportRule \in {"current", "preFinding9", "noRidingFloor", "inMemoryCommit"}

Idx == 1..N
Inf == N + 2                         \* stands for long.MaxValue
Frontier == 1..(N + 1)               \* exclusive frontier values
RowStatus == {"none", "P", "C", "R"} \* absent, Proposed, Committed, RolledBack
                                     \* (the cluster model never writes "R"; a trace can)
Row == Idx \X {"P", "C"}
NoRows == [i \in Idx |-> "none"]

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y

Op == [rows : SUBSET Row, reqSync : BOOLEAN]

NodeState == [disk : [Idx -> RowStatus], pend : [Idx -> RowStatus], queue : Seq(Op),
              commit : Frontier, pendRes : SUBSET Idx,
              present : Frontier, pendPres : SUBSET Idx,
              durPresent : Frontier, pendDur : SUBSET Idx,
              durRes : Frontier, ridingRes : Frontier, ridingFloor : Frontier \cup {Inf},
              resWritten : Frontier, pendResW : SUBSET Idx]

InitNode == [disk |-> NoRows, pend |-> NoRows, queue |-> <<>>,
             commit |-> 1, pendRes |-> {}, present |-> 1, pendPres |-> {},
             durPresent |-> 1, pendDur |-> {},
             durRes |-> 1, ridingRes |-> 1, ridingFloor |-> Inf,
             resWritten |-> 1, pendResW |-> {}]

(* The status a batch writes at index i. The follower plan flattens       *)
(* proposes before resolutions and the last put wins, so a resolution     *)
(* lands over a same-batch Proposed copy.                                 *)
RowAt(rows, i) == IF <<i, "C">> \in rows THEN "C"
                  ELSE IF <<i, "P">> \in rows THEN "P" ELSE "none"

OverlayStatus(base, over) == [i \in Idx |-> IF over[i] # "none" THEN over[i] ELSE base[i]]

(* What a read of the WAL sees: durable rows plus sync-off rows. Queued   *)
(* rows are not visible to a read.                                        *)
Store(n) == OverlayStatus(n.disk, n.pend)

(* Highest k with rows 1..k all Committed / all present (0 when none).    *)
(* Recursive rather than a CHOOSE over 0..N, so a trace with thousands of *)
(* ids is checked in linear time.                                         *)
RECURSIVE ContigFrom(_, _, _)
ContigFrom(st, k, committedOnly) ==
    IF k < N /\ (IF committedOnly THEN st[k + 1] = "C" ELSE st[k + 1] # "none")
    THEN ContigFrom(st, k + 1, committedOnly)
    ELSE k
ContigCommitted(st) == ContigFrom(st, 0, TRUE)
ContigPresent(st) == ContigFrom(st, 0, FALSE)

(* The gap-buffered frontier advance shared by AdvanceCommitFrontier,     *)
(* AdvancePresenceFrontier and AdvanceDurablePresence. Ids below the      *)
(* frontier are duplicates; ids above a gap wait in the buffer; the       *)
(* frontier absorbs the contiguous run. The end state does not depend on  *)
(* the order the ids arrive in, so a closed form is exact.                *)
RECURSIVE FirstGap(_, _)
FirstGap(S, k) == IF k \in S THEN FirstGap(S, k + 1) ELSE k

Advance(f, buf, ids) ==
    LET S == buf \cup {x \in ids : x >= f}
        next == FirstGap(S, f)
    IN <<next, {x \in S : x > next}>>

IsHeld(n, i) == i < n.present \/ i \in n.pendPres

(***************************************************************************)
(* RaftWriteAhead.EnqueueProposeOrCommit (follower append).               *)
(***************************************************************************)

(* The resolved-id guard: a Proposed copy of a locally resolved id is a   *)
(* stale duplicate and is never written.                                  *)
Accepted(n, rows) ==
    {r \in rows : r[2] = "C" \/ ~(r[1] < n.commit \/ r[1] \in n.pendRes)}

CanEnqueue(n, rows) == Accepted(n, rows) # {}

(* The first-durability rule: a Committed row this node does not hold is  *)
(* its first write, not a marker, and must be fsynced.                    *)
RequiresSync(n, rows) ==
    FirstWriteFix /\ \E r \in Accepted(n, rows) : r[2] = "C" /\ ~IsHeld(n, r[1])

FollowerEnqueue(n, rows) ==
    LET w == Accepted(n, rows)
        resolved == {r[1] : r \in {x \in w : x[2] = "C"}}
        c == Advance(n.commit, n.pendRes, resolved)
        p == Advance(n.present, n.pendPres, {r[1] : r \in w})
    IN [n EXCEPT !.queue = Append(@, [rows |-> w, reqSync |-> RequiresSync(n, rows)]),
                 !.commit = c[1], !.pendRes = c[2],
                 !.present = p[1], !.pendPres = p[2]]

(***************************************************************************)
(* FairWalScheduler: write the head op, then WalCompletionRouter's        *)
(* success bookkeeping (MarkDurablyWritten, MarkResolutionWritten).       *)
(***************************************************************************)

(* A batch skips its own fsync only when every row is a Committed marker  *)
(* and no op in it is flagged RequiresSync.                               *)
OpSynced(op) == ~LazyMarkers \/ op.reqSync \/ \E r \in op.rows : r[2] # "C"

OpIds(op) == {r[1] : r \in op.rows}
OpResolved(op) == {r[1] : r \in {x \in op.rows : x[2] = "C"}}

(* Write a batch, given the status it puts at each index ("none" leaves   *)
(* the row alone). A synced write also carries every sync-off row before  *)
(* it on the partition.                                                   *)
WriteRows(n, over, sync) ==
    IF sync THEN [n EXCEPT !.disk = OverlayStatus(OverlayStatus(n.disk, n.pend), over),
                           !.pend = NoRows]
    ELSE [n EXCEPT !.pend = OverlayStatus(n.pend, over)]

WriteOp(n, op, sync) == WriteRows(n, [i \in Idx |-> RowAt(op.rows, i)], sync)

(* IWAL.TruncateProposedLogsAfter: removes every Proposed row above the   *)
(* cut, as a read sees it, durably and at once.                           *)
Truncate(n, after) ==
    LET gone == {i \in Idx : i > after /\ Store(n)[i] = "P"}
    IN [n EXCEPT !.disk = [i \in Idx |-> IF i \in gone THEN "none" ELSE n.disk[i]],
                 !.pend = [i \in Idx |-> IF i \in gone THEN "none" ELSE n.pend[i]]]

(* RaftWriteAhead.MarkDurablyWritten. *)
MarkDurable(n, ids) ==
    LET d == Advance(n.durPresent, n.pendDur, ids)
    IN [n EXCEPT !.durPresent = d[1], !.pendDur = d[2]]

(* RaftWriteAhead.MarkResolutionWritten, line for line. rmin and rmax are *)
(* the lowest and highest resolved id in the batch, -1 when none.         *)
MarkResolution(n, rmin, rmax, sync) ==
    LET riding == IF rmax >= 0 /\ rmax + 1 > n.ridingRes THEN rmax + 1 ELSE n.ridingRes
    IN IF sync
       THEN [n EXCEPT !.ridingRes = riding,
                      !.durRes = Max(n.durRes, riding),
                      !.ridingFloor = Inf]
       ELSE [n EXCEPT !.ridingRes = riding,
                      !.ridingFloor = IF rmin >= 0 /\ rmin < n.ridingFloor THEN rmin ELSE n.ridingFloor]

(* RaftWriteAhead.AdvanceWrittenResolution over the batch's resolved ids:  *)
(* the FINDING 9 fix, a resolution frontier advanced when the resolution   *)
(* is written.                                                            *)
MarkResolvedWritten(n, resolved) ==
    LET w == Advance(n.resWritten, n.pendResW, resolved)
    IN [n EXCEPT !.resWritten = w[1], !.pendResW = w[2]]

RMin(S) == IF S = {} THEN -1 ELSE MinOf(S)
RMax(S) == IF S = {} THEN -1 ELSE MaxOf(S)

(* WalCompletionRouter's success bookkeeping: MarkDurablyWritten, then    *)
(* MarkResolutionWritten.                                                 *)
CompleteOp(n, ids, resolved, sync) ==
    MarkResolvedWritten(MarkResolution(MarkDurable(n, ids), RMin(resolved), RMax(resolved), sync), resolved)

Flush(n) ==
    LET op == Head(n.queue)
        sync == OpSynced(op)
        written == WriteOp([n EXCEPT !.queue = Tail(@)], op, sync)
    IN CompleteOp(written, OpIds(op), OpResolved(op), sync)

(* CompleteFollowerAppend's quorum-integrity gate: Success only when the  *)
(* presence frontier covers the batch.                                    *)
FlushAckSuccess(n) == MaxOf(OpIds(Head(n.queue))) < n.present

(***************************************************************************)
(* The durable frontier a follower reports in every ack                   *)
(* (RaftWriteAhead.GetDurableCommitFrontier, inclusive).                  *)
(***************************************************************************)
(* "preFinding9" bounds the resolved part by the commit frontier, which    *)
(* advances when a resolution is QUEUED: a synced resolution above a lower *)
(* marker that is still queued lifted durRes over that marker (FINDING 9, *)
(* found by this model). "current" also bounds it by resWritten, which    *)
(* advances only when the resolution is WRITTEN.                          *)
Reported(n) ==
    CASE ReportRule = "current" ->
            Min(Min(Min(n.commit, n.resWritten), n.durPresent), Min(n.durRes, n.ridingFloor)) - 1
      [] ReportRule = "preFinding9" ->
            Min(Min(n.commit, n.durPresent), Min(n.durRes, n.ridingFloor)) - 1
      [] ReportRule = "noRidingFloor" ->
            Min(Min(Min(n.commit, n.resWritten), n.durPresent), n.durRes) - 1
      [] ReportRule = "inMemoryCommit" ->
            Min(n.commit, n.durPresent) - 1

(***************************************************************************)
(* Crash and RaftWriteAhead.CompleteRestoreAsync. A crash loses the queue, *)
(* the sync-off rows and all memory; restore re-derives every frontier    *)
(* from the durable disk.                                                 *)
(***************************************************************************)
Restore(disk) ==
    LET commit == ContigCommitted(disk) + 1
        present == IF \E i \in Idx : disk[i] # "none" THEN ContigPresent(disk) + 1 ELSE commit
    IN [disk |-> disk, pend |-> NoRows, queue |-> <<>>,
        commit |-> commit, pendRes |-> {}, present |-> present, pendPres |-> {},
        durPresent |-> present, pendDur |-> {},
        durRes |-> commit, ridingRes |-> commit, ridingFloor |-> Inf,
        resWritten |-> commit, pendResW |-> {}]

Crash(n) == Restore(n.disk)

(* What a restart right now would restore as committed (inclusive). *)
RestorableCommit(n) == ContigCommitted(n.disk)

(***************************************************************************)
(* Node-local invariants.                                                 *)
(***************************************************************************)

(* The leader holds WAL retention on the reported frontier, so a report   *)
(* above what a restart restores lets compaction remove rows this node    *)
(* needs (FINDING 7, FINDING 8).                                          *)
ReportIsDurable(n) == Reported(n) <= RestorableCommit(n)

(* durablePresentIndex certifies rows that are on disk (committed-first-  *)
(* write).                                                                *)
DurablePresenceIsDurable(n) == n.durPresent - 1 <= ContigPresent(n.disk)
=============================================================================
