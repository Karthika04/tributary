# ADR 0003: Replicate with our own Raft, as one Raft group for the whole cluster

- Status: accepted
- Date: 2026-11-26

## Context

A single broker loses availability, and possibly data, when its machine dies. To
survive that, every write has to be stored on several machines before it is
acknowledged, and the machines have to agree on one order of writes even while some of
them crash, restart or lose the network. That is the consensus problem.

Two choices had to be made:

1. **Library or our own implementation?** Apache Ratis is a mature Java Raft library
   used by Apache Ozone. Writing Raft ourselves is more work and more risk.
2. **How many Raft groups?** Kafka (KRaft) runs one Raft group for cluster metadata and
   replicates each partition separately with its own leader. CockroachDB and TiKV run a
   Raft group per data range. The simplest option is a single group for everything.

## Decision

1. **Implement Raft ourselves** (the `raft` module), following section 5 of the Raft
   paper: leader election, log replication and the safety rules. No snapshots or
   membership changes yet.
2. **Use one Raft group for the whole cluster.** Every write the broker accepts
   (create topic, produce, commit offsets) becomes a command in one replicated log.
   Every broker applies the committed commands, in order, to its own local copy of the
   topics. Only the Raft leader serves clients; the others answer `NOT_LEADER` and
   name the leader's address.

## Reasons

- **Understanding is the point of the project.** With a library, the hardest and most
  interesting part of a distributed queue would be a black box. Implementing it means
  every safety rule (the up-to-date vote check, committing only current-term entries,
  the leader's no-op, truncating conflicting entries) is code we wrote, tested, and can
  explain.
- **The Raft core is small.** About 600 lines for the node itself (under 1,000 for the
  whole module including comments), with the log stored in Tributary's own
  crash-safe storage engine (which gained `truncateFrom` for this).
- **It is testable deterministically.** Raft talks to the network through a
  `RaftTransport` interface, so the tests run 3 to 5 nodes in one JVM over a simulated
  network that can drop, partition and crash nodes, including a randomized chaos test
  that checks the safety properties after every round. The deliberately broken vote
  check is caught by three tests every run.
- **One group keeps the system easy to reason about.** One leader, one order of
  writes, one place to look. The state machine is the broker's existing storage code,
  reached through a small `Writes` interface, so standalone and cluster modes share all
  the topic, partition and consumer-group logic.

## Consequences

- **Throughput is bounded by one leader.** All writes go through one broker and one
  log, so adding brokers adds fault tolerance, not write throughput. Kafka's
  per-partition leaders spread that load; that is the natural next step (one Raft group
  per partition, or a metadata group plus per-partition replication).
- **Data is stored twice on each broker:** once in the Raft log and once in the local
  topics. Because the local topics are rebuilt from the Raft log on every start, they
  can never disagree with the cluster, but the Raft log grows forever until snapshots
  are added (planned). For the same reason, retention is turned off in cluster mode:
  deleting data would itself need to be a replicated command.
- **Consumer-group membership is not replicated.** It lives in the leader's memory, like
  Kafka's group coordinator. Committed offsets are replicated, so after a failover
  members rejoin (they get `UNKNOWN_MEMBER` once) and resume from their last commit.
- **Fixed membership.** The cluster's nodes are set at start-up (`--cluster`); adding
  or removing nodes at runtime (Raft section 6) is not implemented.
- **At-least-once remains the guarantee.** If a leader dies after committing a write
  but before answering, the client retries elsewhere and the record is stored twice.
  Acknowledged writes are never lost, which the cluster tests check by killing the
  leader in the middle of a stream of writes.
