------------------------- MODULE KommanderDurability -------------------------
(***************************************************************************)
(* The commit and durability path of one Kommander partition: one stable  *)
(* leader, followers with the write-ahead state of WalDurability, and a   *)
(* network that loses, duplicates and reorders appends.                   *)
(*                                                                         *)
(* Leader side, as the code does it:                                       *)
(*   - a propose is fsynced on the leader and broadcast unanchored         *)
(*     (prevLogIndex 0, the live-propose shape);                           *)
(*   - an entry commits on a durable quorum of Success acks, and the       *)
(*     commit marker is broadcast unanchored;                              *)
(*   - the retention floor follows each follower's REPORTED durable        *)
(*     frontier (ReplicationTracker.SetDurableFrontier); compaction        *)
(*     removes rows through the minimum                                    *)
(*     (HeartbeatDriver.PublishLiveReplicaRetentionFloor);                 *)
(*   - backfill is anchored at the follower's reported frontier and ships  *)
(*     Committed rows; an anchor the leader compacted carries              *)
(*     prevLogTerm = -1 (BackfillSender).                                  *)
(*                                                                         *)
(* Abstractions (slice 1):                                                 *)
(*   - The leader never crashes or changes, so there are no elections, no  *)
(*     inherited entries and no divergent tails.                           *)
(*   - Every append stays in `msgs` once sent: delivery is any number of   *)
(*     times, in any order, or never.                                      *)
(*   - A Success ack reaches the leader's quorum count directly. Delaying  *)
(*     it would only delay a commit the leader may always choose not to    *)
(*     make yet, so no behaviour is lost.                                  *)
(*   - The leader reads a follower's CURRENT report (LeaderObserve). In    *)
(*     the code a report can arrive late, after newer ones (last writer    *)
(*     wins). A late report is an older value of Reported. The history    *)
(*     variable maxObserved keeps the highest value the leader ever saw,   *)
(*     and the invariant StaleReportsAreDurable checks that it never       *)
(*     exceeds what the follower can restore NOW, across crashes. So the   *)
(*     shortcut is justified inside the model instead of assumed.          *)
(*   - Silent-peer windows are not modelled: every follower holds the      *)
(*     floor.                                                              *)
(***************************************************************************)
EXTENDS WalDurability, TLC

CONSTANTS
    Followers,      \* follower ids (model values)
    MaxCrashes,     \* bound on follower crashes in one behaviour
    MaxQueue,       \* bound on each follower's WAL queue
    AnchorFix       \* TRUE: accept a -1 anchor inside the committed prefix (FINDING 6)

ASSUME AnchorFix \in BOOLEAN

Quorum == (Cardinality(Followers) + 1) \div 2 + 1

VARIABLES
    node,           \* [Followers -> NodeState]
    leaderLog,      \* [Idx -> RowStatus], the leader's rows
    nextId,         \* the leader's propose allocator
    compacted,      \* the leader compacted rows 1..compacted
    acks,           \* [Followers -> SUBSET Idx], durable Success acks
    view,           \* [Followers -> 0..N], the leader's record of each reported frontier
    msgs,           \* every append ever sent
    crashes,
    maxObserved     \* history: [Followers -> 0..N], highest report the leader ever saw

vars == <<node, leaderLog, nextId, compacted, acks, view, msgs, crashes, maxObserved>>

AppendMsg == [rows : SUBSET Row, anchor : 0..N, anchorCompacted : BOOLEAN]

Unanchored(rows) == [rows |-> rows, anchor |-> 0, anchorCompacted |-> FALSE]

TypeOK ==
    /\ node \in [Followers -> NodeState]
    /\ leaderLog \in [Idx -> RowStatus]
    /\ nextId \in 1..(N + 1)
    /\ compacted \in 0..N
    /\ acks \in [Followers -> SUBSET Idx]
    /\ view \in [Followers -> 0..N]
    /\ msgs \subseteq AppendMsg
    /\ crashes \in 0..MaxCrashes
    /\ maxObserved \in [Followers -> 0..N]

Init ==
    /\ node = [f \in Followers |-> InitNode]
    /\ leaderLog = NoRows
    /\ nextId = 1
    /\ compacted = 0
    /\ acks = [f \in Followers |-> {}]
    /\ view = [f \in Followers |-> 0]
    /\ msgs = {}
    /\ crashes = 0
    /\ maxObserved = [f \in Followers |-> 0]

-----------------------------------------------------------------------------
(* Leader actions *)

Propose ==
    /\ nextId <= N
    /\ leaderLog' = [leaderLog EXCEPT ![nextId] = "P"]
    /\ nextId' = nextId + 1
    /\ msgs' = msgs \cup {Unanchored({<<nextId, "P">>})}
    /\ UNCHANGED <<node, compacted, acks, view, crashes, maxObserved>>

LeaderCommit(i) ==
    /\ leaderLog[i] = "P"
    /\ Cardinality({f \in Followers : i \in acks[f]}) + 1 >= Quorum
    /\ leaderLog' = [leaderLog EXCEPT ![i] = "C"]
    /\ msgs' = msgs \cup {Unanchored({<<i, "C">>})}
    /\ UNCHANGED <<node, nextId, compacted, acks, view, crashes, maxObserved>>

(* Anchored at the reported frontier. Refused (the snapshot path) when    *)
(* the first row is compacted; reads only a Committed run.                *)
Backfill(f, to) ==
    LET from == view[f] + 1
    IN /\ from <= N
       /\ from > compacted
       /\ to \in from..N
       /\ \A j \in from..to : leaderLog[j] = "C"
       /\ msgs' = msgs \cup {[rows |-> {<<j, "C">> : j \in from..to},
                              anchor |-> from - 1,
                              anchorCompacted |-> from - 1 >= 1 /\ from - 1 <= compacted]}
       /\ UNCHANGED <<node, leaderLog, nextId, compacted, acks, view, crashes, maxObserved>>

(* Any ack or heartbeat reply carries the follower's report. *)
LeaderObserve(f) ==
    /\ view[f] # Reported(node[f])
    /\ view' = [view EXCEPT ![f] = Reported(node[f])]
    /\ maxObserved' = [maxObserved EXCEPT ![f] = Max(@, Reported(node[f]))]
    /\ UNCHANGED <<node, leaderLog, nextId, compacted, acks, msgs, crashes>>

(* Compaction deletes strictly below the floor, and the floor is the      *)
(* minimum reported position + 1, so it removes rows through that         *)
(* minimum. Only committed rows are compacted.                            *)
Compact(c) ==
    /\ compacted < c
    /\ \A f \in Followers : c <= view[f]
    /\ \A j \in 1..c : leaderLog[j] = "C"
    /\ compacted' = c
    /\ UNCHANGED <<node, leaderLog, nextId, acks, view, msgs, crashes, maxObserved>>

-----------------------------------------------------------------------------
(* Follower actions *)

(* FollowerAppendHandler's anchor check. One term, so a present row at    *)
(* the anchor matches. A -1 anchor matches only inside the committed      *)
(* prefix (FINDING 6). A refused append changes no follower state; its    *)
(* LogMismatch ack only carries the report, which LeaderObserve covers.   *)
AnchorOk(n, anchor, anchorCompacted) ==
    IF anchor = 0 THEN TRUE
    ELSE /\ Store(n)[anchor] # "none"
         /\ IF anchorCompacted THEN AnchorFix /\ anchor < n.commit ELSE TRUE

FollowerReceive(f, m) ==
    /\ AnchorOk(node[f], m.anchor, m.anchorCompacted)
    /\ Len(node[f].queue) < MaxQueue
    /\ CanEnqueue(node[f], m.rows)
    /\ node' = [node EXCEPT ![f] = FollowerEnqueue(@, m.rows)]
    /\ UNCHANGED <<leaderLog, nextId, compacted, acks, view, msgs, crashes, maxObserved>>

FollowerFlush(f) ==
    /\ node[f].queue # <<>>
    /\ node' = [node EXCEPT ![f] = Flush(@)]
    /\ acks' = IF FlushAckSuccess(node[f])
               THEN [acks EXCEPT ![f] = @ \cup OpIds(Head(node[f].queue))]
               ELSE acks
    /\ UNCHANGED <<leaderLog, nextId, compacted, view, msgs, crashes, maxObserved>>

FollowerCrash(f) ==
    /\ crashes < MaxCrashes
    /\ node' = [node EXCEPT ![f] = Crash(@)]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<leaderLog, nextId, compacted, acks, view, msgs, maxObserved>>

-----------------------------------------------------------------------------
Next ==
    \/ Propose
    \/ \E i \in Idx : LeaderCommit(i)
    \/ \E f \in Followers, to \in Idx : Backfill(f, to)
    \/ \E c \in Idx : Compact(c)
    \/ \E f \in Followers :
          \/ LeaderObserve(f)
          \/ \E m \in msgs : FollowerReceive(f, m)
          \/ FollowerFlush(f)
          \/ FollowerCrash(f)

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Followers)

-----------------------------------------------------------------------------
(* Invariants *)

(* What every follower reports right now survives a crash right now.      *)
ReportIsDurableAll == \A f \in Followers : ReportIsDurable(node[f])

ViewIsDurable == \A f \in Followers : view[f] <= RestorableCommit(node[f])

(* Compaction never removes a row that some follower needs after a crash, *)
(* so a backfill can always repair it without a snapshot.                 *)
CompactionServable == \A f \in Followers : compacted <= RestorableCommit(node[f])

(* An acknowledged write is durable on a quorum (the leader counts once). *)
CommittedOnQuorum ==
    \A i \in Idx : leaderLog[i] = "C" =>
        Cardinality({f \in Followers : node[f].disk[i] # "none"}) + 1 >= Quorum

DurablePresenceIsDurableAll == \A f \in Followers : DurablePresenceIsDurable(node[f])

(* When the leader's record of a follower is current, the backfill it     *)
(* sends next is accepted: the repair path is open (FINDING 6 wedged it). *)
BackfillAnchorAccepted ==
    \A f \in Followers :
        LET a == view[f] IN
        (a >= 1 /\ a < N /\ a = Reported(node[f])) =>
            AnchorOk(node[f], a, a <= compacted)

(* Justifies LeaderObserve (see the header): no report the leader ever    *)
(* saw is above what the follower can restore now.                        *)
StaleReportsAreDurable ==
    \A f \in Followers : maxObserved[f] <= RestorableCommit(node[f])

(* Hygiene, checked in its own config: what a restart restores never      *)
(* decreases. Stronger than the safety argument needs (FINDING 10 breaks  *)
(* it without breaking StaleReportsAreDurable).                           *)
RestorableCommitMonotone ==
    [][\A f \in Followers : RestorableCommit(node'[f]) >= RestorableCommit(node[f])]_vars

(* Hygiene, checked in its own config: a Committed row on disk never      *)
(* turns back into a Proposed or absent row.                              *)
NoCommittedRowRegression ==
    [][\A f \in Followers, i \in Idx :
          node[f].disk[i] = "C" => node'[f].disk[i] = "C"]_vars
=============================================================================
