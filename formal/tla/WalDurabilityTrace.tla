------------------------- MODULE WalDurabilityTrace -------------------------
(***************************************************************************)
(* Trace validation: replays one simulated node's write-ahead events      *)
(* (one partition) through the WalDurability operators, and checks the    *)
(* code against the model at every event.                                 *)
(*                                                                         *)
(* TraceData is the module Kommander.Tests' WalTraceRecorder writes        *)
(* (run-tlc.sh --trace copies it in under this name). It defines TraceN   *)
(* and TraceLog.                                                           *)
(*                                                                         *)
(* What is input and what is checked:                                      *)
(*   - Writes and truncations update the model's disk. The code's sync     *)
(*     flag decides what a crash loses, exactly as in the cluster model.   *)
(*   - commitIndex and presentIndex are INPUT: they move at enqueue, on    *)
(*     paths slice 1 does not model (leader propose and commit, terms).    *)
(*   - durablePresentIndex, durableResolvedIndex, ridingResolvedIndex,     *)
(*     ridingResolvedFloor, writtenResolvedIndex and the reported durable  *)
(*     frontier are CHECKED:                                               *)
(*     the model computes them from the events and they must equal what    *)
(*     the code logged.                                                    *)
(*   - At a restore, every frontier is checked against Restore(disk).      *)
(*   - At every event, the reported frontier must not exceed what a        *)
(*     restart would restore from the model's disk (ReportIsDurable).      *)
(* The replay stops at the first event the model does not cover.           *)
(***************************************************************************)
EXTENDS WalDurability, TraceData, TLC

VARIABLES
    i,          \* index of the next event
    n,          \* the model's node record
    stopped,    \* "" while validating; otherwise why the replay stopped
    reported    \* TRUE once the summary line is printed

vars == <<i, n, stopped, reported>>

Len_ == Len(TraceLog)
Prev == TraceLog[i - 1]

EventIds(e) ==
    IF Len(e.sparse) > 0 THEN {e.sparse[k] : k \in 1..Len(e.sparse)}
    ELSE IF e.min >= 0 /\ e.max >= e.min THEN e.min..e.max
    ELSE {}

(* The status a write puts at index j: the last row for j in the batch.   *)
LastStatus(rows, j) ==
    LET ks == {k \in 1..Len(rows) : rows[k][1] = j}
    IN IF ks = {} THEN "none" ELSE rows[MaxOf(ks)][2]

RowsOver(e) == [j \in Idx |-> LastStatus(e.rows, j)]

(* commit and present are taken from the code at every event that carries *)
(* state, except a restore, where the model computes them.               *)
WithInputs(m, e) ==
    IF e.hasState THEN [m EXCEPT !.commit = e.commit, !.present = e.present] ELSE m

(* A restore reads the store as it is. After a crash that is the durable  *)
(* disk; after a clean stop the sync-off rows are still in the store and  *)
(* still not fsynced.                                                     *)
RestoreFromStore(m) ==
    [Restore(Store(m)) EXCEPT !.disk = m.disk, !.pend = m.pend]

Apply(e) ==
    CASE e.kind = "write"      -> WriteRows(n, RowsOver(e), e.synced)
      [] e.kind = "truncate"   -> Truncate(n, e.max)
      [] e.kind = "durable"    -> WithInputs(MarkDurable(n, EventIds(e)), e)
      [] e.kind = "resolution" ->
            WithInputs(MarkResolvedWritten(MarkResolution(n, e.min, e.max, e.synced), EventIds(e)), e)
      [] e.kind = "frontiers"  -> WithInputs(n, e)
      [] e.kind = "crash"      -> [n EXCEPT !.pend = NoRows, !.queue = <<>>]
      [] e.kind = "restore"    -> RestoreFromStore(n)
      [] OTHER                 -> n

Init ==
    /\ i = 1
    /\ n = InitNode
    /\ stopped = ""
    /\ reported = FALSE

Step ==
    /\ stopped = ""
    /\ i <= Len_
    /\ IF TraceLog[i].kind = "unsupported"
       THEN /\ stopped' = TraceLog[i].detail
            /\ UNCHANGED n
       ELSE /\ n' = Apply(TraceLog[i])
            /\ UNCHANGED stopped
    /\ i' = i + 1
    /\ UNCHANGED reported

(* Prints how much of the trace was validated, once. *)
Summary ==
    /\ ~reported
    /\ stopped # "" \/ i > Len_
    /\ PrintT(<<"validated events", i - 1, "of", Len_,
                "stopped", IF stopped = "" THEN "at the end" ELSE stopped>>)
    /\ reported' = TRUE
    /\ UNCHANGED <<i, n, stopped>>

Next == Step \/ Summary

-----------------------------------------------------------------------------
(* Checks after the event just applied. *)

Checked == i > 1 /\ stopped = "" /\ Prev.hasState

DurablePresentMatches == Checked => n.durPresent = Prev.durPresent
DurableResolvedMatches == Checked => n.durRes = Prev.durRes
RidingResolvedMatches == Checked => n.ridingRes = Prev.ridingRes
RidingFloorMatches == Checked => n.ridingFloor = Prev.ridingFloor
(* Traces recorded before the FINDING 9 fix carry no writtenRes field. *)
WrittenResolvedMatches == (Checked /\ "writtenRes" \in DOMAIN Prev) => n.resWritten = Prev.writtenRes
ReportedMatches == Checked => Reported(n) = Prev.reported

RestoreCommitMatches == (Checked /\ Prev.kind = "restore") => n.commit = Prev.commit
RestorePresentMatches == (Checked /\ Prev.kind = "restore") => n.present = Prev.present

(* The property itself, on the real run: what the code reported survives *)
(* a crash at this point.                                                 *)
TraceReportIsDurable == Checked => Prev.reported <= ContigCommitted(n.disk)
=============================================================================
