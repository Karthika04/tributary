# ADR 0002: The broker assigns partitions to group members, and fences by ownership

- Status: accepted
- Date: 2026-11-26

## Context

Consumer groups need someone to decide which member reads which partition, and a rule
for who may commit a partition's offset.

Kafka's original protocol does assignment in two round trips: every member sends
JoinGroup, the broker picks one member as "leader", the leader computes the assignment
and sends it back in SyncGroup, and the broker hands each member its share. This lets
applications plug in custom assignors without upgrading brokers, at the cost of a
two-phase protocol and a rebalance barrier that pauses the whole group.

## Decision

1. **The broker computes the assignment** (range or round-robin) when a member joins,
   leaves or times out, and returns each member its partitions in the JoinGroup
   response. One round trip; members learn about later changes through heartbeats.
2. **Commits are fenced by ownership:** the broker saves an offset only if the
   committing member currently owns that partition. Others are returned as `rejected`.

## Reasons

- Simpler protocol and client: no leader election, no SyncGroup, no barrier.
- One source of truth for who owns what, which is also what makes ownership fencing
  possible.
- It follows where Kafka itself went: KIP-848, the "next generation consumer rebalance
  protocol", moved assignment to the broker for the same reasons.

## Consequences

- No barrier means that for up to one heartbeat interval (1 s by default) after a
  rebalance, a partition's old owner may still be processing it while the new owner
  starts from the last commit. Some records can be processed twice. This fits the
  at-least-once guarantee; fencing guarantees the old owner cannot then overwrite the
  new owner's committed progress.
- Custom assignment strategies require a broker change.
