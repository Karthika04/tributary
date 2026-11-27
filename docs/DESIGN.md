# Tributary design

Tributary is a distributed, fault-tolerant message queue in the style of Apache Kafka,
written from scratch in Java. This document explains what it does, how it is built,
and why each major decision was made.

Status: **v5: storage engine, wire protocol, broker, client library, CLI, consumer groups
and Raft replication across a 3-broker cluster complete, chaos-tested and fuzzed.**
Later sections are marked *planned*.

## 1. Goals

- **Durable, ordered streams.** Producers append messages to a topic partition; every
  message gets a unique, increasing offset; consumers read from any offset.
- **Survive crashes.** After a process crash or power loss, no acknowledged message is
  lost or corrupted, and damaged data is detected rather than returned.
- **Survive machine failures.** In a 3-broker cluster every write is replicated with
  Raft before it is acknowledged, so the cluster keeps working, and loses nothing it
  acknowledged, when any one broker dies.
- **High throughput.** Batched writes, append-only files and an O(1) index.
- **Evidence over claims.** Every guarantee above is backed by automated tests,
  including fault-injection ("chaos") tests against real broker processes and
  fuzz tests of every decoder (section 10).

## 2. Non-goals

- Exactly-once delivery and transactions (we provide at-least-once).
- Log compaction by key (Kafka's "compacted topics").
- Kafka wire-protocol compatibility. Tributary uses its own, simpler binary protocol.
- Multi-datacenter replication.

## 3. Architecture

```
  +--------------------+        binary protocol over TCP         +---------------------------+
  | client module      |  -------------------------------------> | broker module             |
  |  Producer          |   [length][header][body] frames         |  BrokerServer (sockets,   |
  |  Consumer          |  <-------------------------------------  |    virtual threads)       |
  |  TributaryClient   |                                          |  RequestHandler (logic)   |
  +--------------------+                                          |  TopicManager             |
  | cli: `tributary`   |                                          |   -> Partition (+ long    |
  +--------------------+                                          |      polling)             |
            \                                                     +-------------+-------------+
             \  both use: protocol module (messages, codec,                     |
              \           framing, compression, partitioner)    +-------------v-------------+
                                                                 | storage module            |
                                                                 |  Log -> Segment -> Store  |
                                                                 |                 -> Index  |
                                                                 +---------------------------+
```

| Module | What it is | Depends on |
|---|---|---|
| `storage` | Segmented, crash-safe commit log | JDK only |
| `protocol` | Message types, binary codec, framing, compression, partitioner | JDK only |
| `raft` | Raft consensus: elections, log replication, safety rules | storage |
| `broker` | TCP server, topics and partitions, long polling, retention, consumer groups, cluster mode | storage, protocol, raft, client |
| `client` | `TributaryClient`, batching `Producer`, polling `Consumer`, multi-broker failover | protocol |
| `cli` | The `tributary` command-line tool | client |
| `chaos` | Chaos testing: real broker processes, fault injection, a correctness checker | client, broker |

The broker uses the client library to talk to other brokers in a cluster (section 9).

## 4. Storage engine

### 4.1 The log

Each partition is one `Log` in its own directory. A log is a sequence of **segments**.
Only the newest segment (the *active* one) is written to; older ones are read-only.

```
data/orders-0/
  00000000000000000000.log      records 0..999   (read-only)
  00000000000000000000.index
  00000000000000001000.log      records 1000..   (active)
  00000000000000001000.index
  .lock                         held while the log is open
```

File names are the segment's first offset, zero-padded to 20 digits so they sort
correctly as text.

**Why segments?** Retention then means deleting whole old files, which is instant and
never touches data that is still needed.

### 4.2 Record format

```
| length 4 | crc 4 | offset 8 | timestamp 8 | keyLen 4 | key | valueLen 4 | value |
```

- `length` lets a reader skip to the next record without parsing this one.
- `crc` is CRC32C over everything after it (offset through value). CRC32C is
  hardware-accelerated on modern CPUs and is what Kafka uses.
- `keyLen = -1` means a null key.
- The offset is stored inside the record, so recovery can verify that records are
  in order, not just that each is undamaged.

### 4.3 The index

One 8-byte entry per record: `[relativeOffset: int][filePosition: int]`, where
relative offset = offset - segment base offset.

**Decision: dense index (one entry per record) instead of Kafka's sparse index.**
Looking up offset N is a single multiplication, O(1), with no binary search or scan.
The cost is a larger index file: 8 bytes per record. For small messages this is
noticeable, so it is a deliberate trade of disk for simplicity and lookup speed.

The index file is **memory-mapped**: reads are plain memory accesses with no system
call. While a segment is active the file is pre-sized to its full capacity, so it is
mapped only once. On a clean close it is truncated to the entries actually written.

### 4.4 Writing

1. Validate every record's size first, so a batch with an oversized record is rejected
   before anything is written.
2. Take as many records as fit in the active segment. When the segment is full,
   **roll**: flush and seal it, and create a new one. A batch bigger than a segment
   simply continues in the next segment.
3. Encode each chunk into one buffer and write it with **one** write call.
4. Add index entries.

Step 3 before step 4 matters: if the process dies between them, the log file has the
data and recovery rebuilds the index from it. The two can never permanently disagree.

**Durability setting.** By default, writes go to the operating system's page cache
and reach the physical disk when the OS decides, or when the segment rolls or closes.
`flushEveryAppend = true` forces every write to disk before returning: the safest
option, and much slower. Kafka makes the same default choice and relies on replication
across machines for durability, which Tributary will do too.

### 4.5 Reading

Find the segment with `TreeMap.floorEntry(offset)`, look up the file position in its
index, read the frame, verify the checksum, and check the stored offset matches the
one requested. Reading at the end offset returns an empty list, meaning "caught up".

### 4.6 Crash recovery

`close()` writes a `.clean-shutdown` marker as its very last step. On open:

- **Marker present:** the last shutdown finished; trust the index files.
- **Marker missing:** the process crashed. Rebuild the newest segment's index by
  scanning its log file from the start, checking every record's checksum and offset.
  At the first record that is cut off or damaged, truncate the file there.

Truncating is correct because a record that was not completely written was never
acknowledged to the producer. Older segments were flushed when they were sealed, so
only the newest one needs scanning. This is the same approach Kafka uses.

Defence in depth: even when the marker is present, opening a segment checks that its
index's last entry points exactly at the last complete record in the log file, and
rebuilds the index if not. A missing index file is also rebuilt.

### 4.7 Concurrency

A `ReentrantReadWriteLock` per log:

- **Appends, roll, retention, close:** write lock (one at a time). This is what makes
  offsets strictly sequential.
- **Reads:** read lock (any number at once). File reads use positional
  `FileChannel.read(buffer, position)`, which has no shared file pointer, so
  concurrent readers do not interfere.

A `FileLock` on `.lock` stops two processes from opening the same log directory.

### 4.8 Retention

`applyRetention()` (called on a timer by the broker) deletes the oldest segments while:

- their newest record is older than `retentionMs`, or
- the log is larger than `retentionBytes`.

It never deletes the active segment and only ever deletes whole segments, oldest
first, so the remaining offsets stay contiguous.

### 4.9 Limits

- Segment files are addressed with `int` positions: at most 2 GiB per segment.
- Records per segment are capped by the index size (`indexBytes / 8`).
- Java 21 cannot unmap a memory-mapped file on demand, so the index relies on
  truncate-while-mapped, which is safe on Linux and macOS (not Windows).

## 5. Wire protocol

### 5.1 Framing

TCP is a byte stream with no message boundaries, so every message is a **frame**:
`[length: int][payload]`. A reader first reads 4 bytes, then exactly that many more.
Frames over 16 MiB are refused, so a corrupt or hostile length cannot make the broker
allocate gigabytes.

### 5.2 Messages

```
request  = [apiKey: short][version: short][correlationId: int][clientId: string][body]
response = [correlationId: int][apiKey: short][error: short][errorMessage: string][body]
```

| API | Request body | Response body |
|---|---|---|
| CREATE_TOPIC | topic, partitions | (none) |
| METADATA | (none) | list of (topic, partition count) |
| PRODUCE | topic, partition, record block | base offset |
| FETCH | topic, partition, offset, maxRecords, maxWaitMs, compression | start offset, end offset, record block |
| LIST_OFFSETS | topic, partition | start offset, end offset |
| JOIN_GROUP | group, member id (empty if new), topics, session timeout, assignor | member id, generation, assigned partitions |
| HEARTBEAT | group, member id, generation | (none), or REBALANCE_IN_PROGRESS |
| LEAVE_GROUP | group, member id | (none) |
| COMMIT_OFFSETS | group, member id, (topic, partition, offset) list | partitions rejected because not owned |
| FETCH_OFFSETS | group, partitions | committed offset per partition (-1 if none) |

- **correlationId:** chosen by the client and echoed by the broker, so a response can
  be matched to its request. The client checks it and drops the connection on a
  mismatch rather than ever handing back the wrong answer.
- **version:** lets the protocol evolve without breaking old clients. Unknown versions
  get an `UNSUPPORTED_VERSION` error.
- **errors:** 16 error codes, each marked retriable or not, so clients know whether
  trying again could help. A body that cannot be decoded gets `INVALID_REQUEST` and the
  connection stays usable; an unreadable header closes the connection.

Requests and responses are Java **sealed interfaces** with one record per type. A
`switch` over them is checked by the compiler, so adding a request type and forgetting
to handle it anywhere is a compile error.

### 5.3 Record blocks and compression

Records travel in blocks: `[compression][uncompressedSize][payloadSize][payload]`.
Compressing a whole block shrinks repetitive data (JSON, logs) far more than
compressing records one by one; the tests show more than 4x for typical JSON. GZIP is
used because it ships with the JDK. The declared uncompressed size is enforced while
decompressing, which blocks "zip bomb" payloads.

Storage keeps records uncompressed; compression is for the network only.

### 5.4 Partitioning

A record with a key goes to `murmur2(key) mod partitions`, the same function as
Kafka's default partitioner (verified against Kafka's own test vectors). Same key,
same partition, so per-key order is preserved. Keyless records stick to one partition
until its batch is sent, then move to the next, which keeps batches large.

**Decision: our own binary protocol over TCP instead of gRPC.** See
[ADR 0001](adr/0001-custom-tcp-protocol.md).

## 6. Broker

### 6.1 Threading: one virtual thread per connection

Each connection is served by its own **virtual thread** (Java 21) running a simple
loop: read a frame, handle it, write the response. Virtual threads are cheap (a few KB
each) and park without blocking an OS thread while they wait on the network, so this
easy-to-read, blocking style scales to many thousands of connections.

### 6.2 Topics on disk

```
data/
  orders/        <- topic
    0/           <- partition 0, one storage Log
    1/
```

The folders are the source of truth: on startup every topic folder with partition
folders `0..N-1` is loaded. Names allow letters, digits, `.`, `_` and `-` (safe as
folder names everywhere; `..` is rejected).

### 6.3 Timestamps

The broker stamps each record with its own clock when storing it ("LogAppendTime" in
Kafka terms). Timestamps within a partition therefore never go backwards, which the
time-based retention relies on.

### 6.4 Long polling

A caught-up consumer sends `FETCH(offset = end, maxWaitMs = 500)`. Instead of
answering "nothing yet" and being asked again in a tight loop, the broker parks the
request on the partition's condition variable and wakes it the moment a producer
appends (or the wait runs out). New messages reach consumers within milliseconds while
idle consumers cost almost nothing. The check-then-wait happens under the same lock
that `append` signals under, so a record cannot slip in unnoticed between them.

### 6.5 Graceful shutdown

On Ctrl+C (a JVM shutdown hook): stop accepting, close client sockets, wake every
long-polling fetch, wait for in-flight requests to finish, then close every log (which
writes the clean-shutdown marker). Handler threads are deliberately **never
interrupted**: interrupting a thread during a `FileChannel` write closes the channel,
which would break the log.

### 6.6 Retention

A background thread calls `applyRetention()` on every partition on a timer
(default every 60 s).
In cluster mode retention is off, because deleting data on one broker would make it
disagree with the others; see section 9.3.

## 7. Client library

- **`TributaryClient`:** one connection, connects lazily, reconnects after a network
  error, sends one request at a time.
- **`Producer`:** `send()` returns a `CompletableFuture` immediately; a background
  sender ships a partition's batch when it reaches `batchBytes` or has waited
  `lingerMs`. Retriable errors are retried with exponential backoff (100 ms, 200 ms,
  400 ms... up to 5 s). Batches go out one at a time and a batch is retried before the
  next one is sent, so order within a partition is kept. Delivery is **at-least-once**:
  if a connection drops after the broker stored a batch but before the answer arrived,
  the retry stores it again.
- **`Consumer`:** keeps a position per partition. `poll()` first asks every partition
  for data without waiting (rotating the starting partition so none is starved), and if
  all are caught up, long-polls one. If its position was deleted by retention it
  resets to the oldest (or newest) offset, per `autoOffsetReset`. With a group id it
  becomes a group member (section 8); without one, partitions are chosen by hand and
  positions live only in memory.

## 8. Consumer groups

### 8.1 What they are for

Several copies of a service read one topic as a team: each partition goes to exactly
one member, so the work is shared and each record is processed by one member. Different
groups are independent, and each sees every record (one topic can feed billing,
shipping and analytics at once). When a member joins, leaves or dies, its partitions
move to the others automatically, and the group's committed progress lets the new owner
carry on where the old one stopped.

### 8.2 The membership protocol

```
consumer A                 broker (GroupCoordinator)              consumer B
    | JOIN(billing, -, [orders]) ->|                                  |
    |<- member A, gen 1, [p0..p3]  |                                  |
    |                              |<- JOIN(billing, -, [orders]) ----|
    |                              |-- member B, gen 2, [p2,p3] ----->|
    | HEARTBEAT(A, gen 1) -------->|                                  |
    |<- REBALANCE_IN_PROGRESS -----|                                  |
    | COMMIT(p2, p3 progress)      |  (rejected: B owns them now)     |
    | JOIN(billing, A, [orders]) ->|                                  |
    |<- member A, gen 2, [p0,p1] --|                                  |
```

- **Join:** the broker gives a new member an id, bumps the group's **generation** and
  reassigns every partition among the current members.
- **Heartbeat:** members send one every second (from inside `poll()`). A member whose
  generation is behind gets `REBALANCE_IN_PROGRESS` and joins again, with its existing
  id, to learn its new partitions.
- **Leave:** `Consumer.close()` sends it, so partitions move immediately.
- **Session timeout:** a background check removes members with no heartbeat for their
  session timeout (default 10 s) and rebalances. This covers crashes, where no Leave
  is ever sent.

**Decision: the broker computes assignments**, in one round trip, instead of Kafka's
original two-step protocol where one consumer computed them. See
[ADR 0002](adr/0002-broker-side-group-assignment.md).

### 8.3 Assignment strategies

Both are deterministic (members sorted by id) and give every partition to exactly one
subscribed member. Both match the examples in Kafka's documentation, which the tests
check.

- **range:** per topic, contiguous blocks of partitions per member. Partition N of two
  co-keyed topics lands on the same member.
- **roundrobin:** every partition of every topic dealt out in turn. Most even overall.

### 8.4 Committed offsets

A committed offset means "every record before this was processed". Commits are appended
to the internal topic `__consumer_offsets` as `key = group/topic/partition,
value = offset`, so they get the storage engine's durability and crash recovery for
free; the broker replays the topic on startup. The topic is hidden from topic listings,
and clients cannot create topics starting with `__` or produce to them.

Known limitation: without log compaction this topic only grows. Kafka compacts it to the
newest record per key.

### 8.5 Fencing zombies

A consumer that froze (say, a long GC pause) may not know it lost its partitions. If it
then committed, it could overwrite the new owner's progress. So **only the member that
currently owns a partition may commit it**; anything else comes back in the response's
`rejected` list and is not saved.

### 8.6 Delivery guarantee: at-least-once

Auto-commit (default every second, and on close) commits positions reached by earlier
`poll()` calls, so a record is only committed after the application had it. If a
consumer crashes before committing, or a rebalance moves a partition before the commit,
the new owner starts from the last commit and some records are processed again. Records
are never skipped. The CLI's `--max-messages` rewinds positions for records it fetched
but did not print, so even that never commits past unprocessed records.

## 9. Replication with Raft

Why our own Raft, and why one Raft group: [ADR 0003](adr/0003-raft-from-scratch-single-group.md).

### 9.1 The idea

Brokers in a cluster keep one shared, replicated log of *commands*: "create topic
orders with 3 partitions", "append these records to orders-1 with timestamp T",
"group billing committed offset 42 for orders-1". Raft makes every broker store the
same commands in the same order. Each broker then *applies* the committed commands to
its own local topics, using exactly the code a single broker uses. Same commands, same
order, deterministic code: every broker ends up with byte-for-byte the same data.

```
  client --produce--> leader broker
                        |  1. encode as a command, append to its Raft log
                        |  2. send to followers (AppendEntries)        ---> follower 2
                        |  3. a majority (2 of 3) has stored it: committed  ---> follower 3
                        |  4. apply: append to local orders-1, get offset 17
  client <--offset 17-- |  5. answer
                        followers apply the same command when they learn it is committed
```

The leader only answers after step 3, so an acknowledged write is on at least two of
the three machines. Any one can die and the write survives on a majority, and Raft's
election rule (below) guarantees the next leader has it.

### 9.2 The raft module

`RaftNode` implements section 5 of the Raft paper:

- **Terms and elections.** Each node waits a random 400 to 800 ms for a heartbeat. If
  none arrives it becomes a candidate, increments its term and asks for votes. A node
  votes at most once per term, and **only for a candidate whose log is at least as
  up to date as its own** (higher last term, or same last term and at least as long).
  This rule is what keeps committed entries from being lost: a committed entry is on a
  majority, any winning candidate needs votes from a majority, the two overlap, and the
  overlapping voter refuses a candidate that lacks the entry.
- **Replication.** The leader runs one virtual thread per follower, sending
  AppendEntries with the entries the follower is missing (up to 500 entries or 4 MB
  per request) or an empty heartbeat every 100 ms. Each request carries the index and
  term of the entry just before the new ones; a follower whose log disagrees there
  refuses, with a hint of where its log ends, and the leader steps back. When a
  follower has a conflicting entry (from an old leader that never committed it), it
  truncates its log from that point and takes the leader's entries.
- **Commit rule.** The leader commits an index once a majority has stored it, but
  **only counts entries from its own term** (Raft figure 8). Older entries are
  committed indirectly when a current-term entry after them commits. To make that
  happen promptly, a new leader appends an empty *no-op* entry as soon as it wins.
- **Safe reads.** A new leader may not yet know which entries are committed. It only
  reports itself *ready* once its no-op is committed and applied, and the broker serves
  clients only from a ready leader, so nobody reads stale data.
- **Durability.** The current term and vote are written to `raft-meta` with a write to
  a temporary file, `fsync`, and an atomic rename, so a crash can never leave a node
  that voted twice in one term. The log itself is a Tributary storage `Log`, one
  record per entry (term plus command), so it inherits the checksums and crash
  recovery from section 4.
- **Group commit.** A node never counts an entry as stored, for itself or in its
  answer to a leader, until it is forced to disk. But one `fsync` per proposal, made
  while holding the node's lock, stalls heartbeats on a slow disk (on a Mac an
  `fsync` can take several milliseconds), and
  followers then start needless elections. So the leader appends without waiting,
  and a flusher thread forces the disk *outside* the lock, covering every entry that
  arrived since the last flush. The leader counts its own copy toward the majority
  only up to what is flushed. A follower flushes once per AppendEntries batch before
  it answers. This was found by the tests on a Mac; the same failure reproduces on
  Linux by adding a 5 ms delay to every flush, and with the fix the Raft and cluster
  tests pass even at 20 ms.
- **CheckQuorum.** A leader steps down when a majority has not answered it within the
  longest election timeout. Without this, a leader cut off from its peers but still
  reachable by clients keeps accepting writes it can never commit, and clients wait
  out their own timeouts (with default settings, possibly the whole partition). With
  it, waiting requests fail with `NOT_LEADER` within about a second and clients find
  the new leader. Found by the chaos tests (section 10).
- **PreVote** (Raft thesis section 9.6). Before raising its term for an election, a
  node asks the others whether they *would* vote for it, which changes no state. A
  node that heard from a live leader within the shortest election timeout says no.
  Without this, a follower cut off for a few seconds keeps timing out and raising its
  term, and on its return that higher term forces the healthy leader to step down:
  the chaos tests saw one returning follower cost three elections. Also found by the
  chaos tests.
- **Applying.** A separate applier thread applies committed entries in order and
  completes the `CompletableFuture` returned by `propose()` with the state machine's
  result (for a produce, the base offset). If the node loses leadership first, the
  future fails with `NotLeaderException`; the command may still commit under the next
  leader, which is one source of at-least-once duplicates.

Not implemented yet: snapshots and log compaction, and membership changes.

### 9.3 Cluster mode in the broker

Start each broker with the full member list:

```bash
tributary-broker --node-id 1 --cluster 1=host1:9292,2=host2:9292,3=host3:9292 --data-dir ./data
```

- **Separate peer addresses.** `--peer-addresses 2=10.0.0.2:9292,...` tells a broker how
  to reach a peer where that differs from the `--cluster` address clients are given,
  like Kafka's `listeners` vs. `advertised.listeners`. The chaos tests use it to route
  every one-way link between brokers through its own proxy.
- **One port for everything.** Brokers send each other `RAFT_VOTE` and `RAFT_APPEND`
  requests over the same Tributary protocol that clients use, through the client
  library (`BrokerRaftTransport`). No second network stack.
- **Writes go through a `Writes` interface.** A single broker uses `LocalWrites`
  (straight to disk). A cluster member uses `RaftWrites`, which encodes the write as a
  command (`Commands`), proposes it, and waits up to 10 seconds for it to commit. The
  `ClusterStateMachine` decodes committed commands and hands them to `LocalWrites`.
  The produce command carries the leader's timestamp, so every broker stores the same
  `LogAppendTime`.
- **Validation happens before proposing.** Bad topic names, unknown partitions and
  oversized batches are rejected by the leader before anything enters the log. A
  command that still fails when applied (say, two concurrent creates of the same
  topic) fails the same way on every broker, so the brokers stay identical.
- **Only the leader serves clients.** Everything else gets `NOT_LEADER` with
  `leader is host:port`. A cluster produce is capped at 8 MB so its command fits in one
  replication request.
- **Local topics are rebuilt on start.** A cluster member deletes `data/state` on start
  and replays the Raft log into it. That keeps it impossible for a broker's topics to
  drift from the agreed log, at the cost of replay time (fixed later by snapshots).
  Retention is off in cluster mode for the same reason.
- **Groups.** Committed offsets are replicated (they are commands). Group membership
  stays in the leader's memory, as in Kafka; after a failover members get
  `UNKNOWN_MEMBER` once, rejoin, and resume from their committed offsets.

### 9.4 The client side

`ClientConfig` accepts a list of brokers (`--bootstrap h1:p1,h2:p2,h3:p3` on the CLI).
On `NOT_LEADER` the client goes straight to the named leader; on a network error, a
broker shutting down, or `NOT_LEADER` with no leader yet, it moves to the next broker
with exponential backoff (100 ms, doubling, capped at 1 s), up to 5 attempts by default.
The producer's own retries sit on top of that, so a producer rides through a leader
election, which takes around a second, without the application noticing.

## 10. Chaos testing and fuzzing

Unit and cluster tests check the cases we thought of. Chaos testing checks the ones we
did not: it breaks a real cluster in many ways, at random moments, while clients are
writing, and then checks every guarantee. It found two real Raft problems (CheckQuorum
and PreVote, section 9.2); fuzzing found three decoder bugs.

Why real processes and our own proxy rather than Docker, Testcontainers and Toxiproxy:
[ADR 0004](adr/0004-chaos-with-processes-and-our-own-proxy.md).

### 10.1 The harness

```
              clients connect to every broker directly
        +---------------------------------------------------+
        v                         v                         v
   broker 1 (JVM)            broker 2 (JVM)            broker 3 (JVM)
        |  proxy 1->2 ---------->  |  proxy 2->3 ---------->  |
        |  <---------- proxy 2->1  |  <---------- proxy 3->2  |
        +------ proxy 1->3, proxy 3->1 ----------------------+
```

- **Each broker is its own JVM** (`BrokerProcess`), so a crash is a real `kill -9`:
  no shutdown hook, no final flush, sockets torn down by the operating system.
- **Each one-way link has its own proxy** (`ChaosProxy`), and each broker reaches its
  peers through its own proxies (`--peer-addresses`). Cutting proxy 1->2 and 2->1
  separates exactly brokers 1 and 2. A cut link drops open connections and
  *black-holes* new ones: they are accepted but nothing comes back, so requests hang
  until the sender's timeout, the way a dead network behaves.
- **Clients bypass the proxies**, so a leader cut off from its peers can still be
  reached by clients. That is the hard case: a leader that is alive and answering but
  can no longer get a majority behind anything.

### 10.2 Faults

| Fault | What it does | What must hold |
|---|---|---|
| `KILL_LEADER` | `kill -9` the leader | a new leader is elected; nothing acknowledged is lost |
| `KILL_FOLLOWER` | `kill -9` a follower | writes continue on the other two |
| `KILL_ALL` | `kill -9` all three at once, then restart them | every acknowledged write is on disk |
| `PARTITION_LEADER` | cut the leader off from both peers | it acknowledges nothing it cannot replicate; the other two elect a leader |
| `PARTITION_FOLLOWER` | cut a follower off | it catches up after the heal, without disrupting the leader |
| `PAUSE_LEADER` | freeze the leader with `SIGSTOP`, later `SIGCONT` | it wakes up still believing it leads, and must step down |
| `SLOW_NETWORK` | 50 to 250 ms of delay on every link | everything still works, more slowly |

### 10.3 A round, and the checker

1. Create a fresh topic with 3 partitions.
2. Three producers write `p<k>-0`, `p<k>-1`, ... to partition `k`, one record per
   request and one request at a time, recording for every value whether it was
   acknowledged, and at which offset.
3. After 0.8 s, inject a fault. Hold it for 1.5 to 3 s, then heal everything:
   reconnect links, wake frozen brokers, restart dead ones. Keep writing for 2 s more.
4. Read every partition back and check, in the spirit of Jepsen's queue checkers:
   - **Lost** (violation): acknowledged, but not stored.
   - **Wrong offset** (violation): the offset a producer was given holds another record.
   - **Out of order** (violation): a producer's values stored in a different order
     than sent (first copies).
   - **Foreign** (violation): a value in a partition its producer never wrote to.
   - **Duplicates** (allowed, at-least-once) and **failed but stored** (allowed: the
     producer gave up, but the write had committed).
5. After a cluster's last round, stop all three brokers and compare their Raft logs
   on disk: any two must agree entry by entry up to the shorter one's length (Raft's
   Log Matching property).

The checker has its own unit tests, which feed it each kind of violation by hand.
Every round also records the **longest gap with no acknowledged write**: how long the
fault made the cluster unavailable for writes.

### 10.4 Results: 500 rounds

50 fresh clusters of 10 rounds each (seed 2026), every fault about 71 times, on a
2-CPU Linux machine. The full per-round results are in
[docs/chaos/results-seed-2026.csv](chaos/results-seed-2026.csv).

| Fault | Rounds | Acknowledged writes | Lost | Write stall, median | 95th percentile |
|---|---:|---:|---:|---:|---:|
| `KILL_LEADER` | 72 | 390,522 | 0 | 1.5 s | 1.6 s |
| `KILL_FOLLOWER` | 72 | 515,440 | 0 | 13 ms | 71 ms |
| `KILL_ALL` | 72 | 93,942 | 0 | 4.5 s | 4.6 s |
| `PARTITION_LEADER` | 71 | 399,374 | 0 | 0.9 s | 1.1 s |
| `PARTITION_FOLLOWER` | 71 | 537,694 | 0 | 11 ms | 71 ms |
| `PAUSE_LEADER` | 71 | 260,258 | 0 | 2.4 s | 3.1 s |
| `SLOW_NETWORK` | 71 | 267,182 | 0 | 318 ms | 492 ms |
| **Total** | **500** | **2,464,412** | **0** | | |

- **No violations of any kind**: nothing lost, no acknowledged offset holding the wrong
  record, nothing out of order, nothing foreign, and all 50 sets of Raft logs agreed.
- **Duplicates: 284** (about 0.01% of acknowledged writes), from retries after
  timeouts, as at-least-once allows. 486 writes failed; 169 of those had committed
  anyway.
- **Write stall** is the longest stretch with no acknowledged write. Losing the leader
  costs one election (about 1.5 s for a crash, 0.9 s for a partition, thanks to
  CheckQuorum). Losing a follower costs nothing noticeable. `KILL_ALL` includes
  starting three new JVMs. A frozen leader is bounded by the time it stays frozen and
  the client's request timeout (5 s here): a process that cannot run cannot step down.
- **Outliers.** Four rounds, all in cluster 10, stalled for about 6.5 s, each with
  exactly one duplicate per producer: requests that committed but were not answered
  within the 5 s timeout. A targeted rerun of 36 rounds did not reproduce it, and
  making every broker's `fsync` wait 6 s produced the same signature (6.4 s stall, one
  duplicate per producer), so the likely cause is a stall in the test machine's disk,
  which all three brokers shared. The cluster behaved correctly through it: no write
  was acknowledged before it was on disk, and none was lost.

Run it yourself:

```bash
./gradlew :chaos:installDist
chaos/build/install/chaos-runner/bin/chaos-runner --clusters 50 --rounds 10 --seed 2026
# options: --faults KILL_LEADER,PAUSE_LEADER  --hold-ms 8000  --keep (keep data and broker logs)
```

### 10.5 Fuzzing

`CodecFuzzTest` takes valid requests and responses and damages them in seeded random
ways: flipped bits, cut-offs, garbage at the end, extreme values (-1, `Integer.MAX_VALUE`)
written over length and count fields, repeated slices, and pure noise. Decoding must
either succeed or throw `ProtocolException`. Anything else (a `NullPointerException`, a
`NegativeArraySizeException`, an `OutOfMemoryError`) is a bug: on the broker it would end
the connection with an unhelpful error, and a client would see an exception it does not
expect. `RecordCodecFuzzTest` does the same for on-disk frames, recomputing the checksum
on half of the inputs so the damage reaches the field parsing behind it.

It found three bugs in the first few thousand inputs:

- A JoinGroup whose topic list contained a null string: `NullPointerException`.
- A response holding an invalid value (a partition with no topic) escaped the client as
  `IllegalArgumentException`.
- **A 150-byte message declaring a 2 GB compressed block made the receiver allocate
  2 GB.** The zip-bomb guard preallocated the declared size before decompressing
  anything: the guard was itself the bomb. Blocks are now decompressed in chunks and
  capped at 64 MiB.

Each bug's exact input is kept as a regression test. After the fixes, 300,000 damaged
messages per decoder and 1,000,000 damaged record frames passed. The seed is fixed, and
`-Dfuzz.iterations=N` runs longer.

## 11. Testing

164 automated tests across the modules.

Storage (47), including:

- Every single-bit flip in a record is detected by the checksum.
- A half-written record at the end of the file is dropped after a crash, and a
  second crash afterwards loses nothing.
- A damaged last record, an index left at full size, and a missing index are all
  recovered from.
- Damaged bytes in an old segment raise an error instead of being returned.
- 8 writer threads and 4 reader threads at once: 16,000 records, no gaps, no
  duplicates, every read returns the offset it asked for.
- 100,000 records across many segments read back exactly.
- Retention by time and size, including across a restart.
- A batch bigger than a segment continues across segments.
- Fuzzing: 1,000,000 damaged record frames, half with a recomputed checksum, fail only
  as `CorruptRecordException` (section 10.5).
- Truncating the tail (for Raft) works, survives a restart and a crash, and later
  appends reuse the freed offsets.

The storage tests were also checked by deliberately breaking the code (removing the
truncation step, skipping crash recovery) and confirming the right tests fail.

Protocol (22): every message type round-trips with and without compression; cut-off,
padded, wrong-version and lying-compression-size inputs are rejected; murmur2 matches
Kafka's published values; keys spread evenly; fuzzing (section 10.5), including the
exact inputs of the three bugs it found.

Raft (14), over a simulated network: exactly one leader per term; a vote is refused to
a candidate whose log is behind; a node that missed committed entries can never become
leader; committed commands reach every node in order; followers refuse proposals and
name the leader; a new leader keeps every committed command; an isolated leader steps
down before the partition heals (CheckQuorum) and its uncommitted entries are discarded
when it rejoins; a follower returning from a partition does not disrupt the leader, and
a pre-vote changes nothing (PreVote); a lagging follower
catches up; terms, votes and logs survive restarting every node; and a randomized
chaos test (5 nodes, 25 rounds of crashes, restarts and partitions, fixed seed) checks
after every round that no two nodes ever disagree on a committed entry. Breaking the
vote rule on purpose makes the deterministic vote test fail on every run, and usually
two more.

Cluster (7), with three real brokers on localhost: every broker ends up with identical
data; a follower redirects clients to the leader; **killing the leader in the middle of
300 writes loses none of the acknowledged ones**; a restarted broker catches up on
what it missed, including topics created while it was down; the old leader rejoins as
a follower; committed group offsets survive failover; without a majority nothing is
acknowledged, and writes resume when a majority returns. Making the broker acknowledge
a produce before it commits makes three of these fail.

Broker (21), against a real broker on a random port: topic errors; broker timestamps;
compression both ways; a waiting fetch returns as soon as data arrives and gives up on
time; shutdown wakes waiting fetches; data survives a restart; background retention;
50 concurrent clients; malformed requests get an error and the connection keeps
working; garbage and oversized frames close the connection; cluster and peer-address flags.

Client (12): every record gets a unique, dense offset; keyed records stay on their
partition in order; unknown topics fail the future; the producer keeps retrying while
the broker is down and succeeds once it is back; the consumer reads every partition
with no gaps; latest vs earliest; poll wakes on new data; seek; reset after retention
deletes the position; compression end to end.

Consumer groups (23): both assignors match Kafka's documented examples; every partition
is owned once; idle members when there are more members than partitions; rebalance on
join, leave, subscription change and session expiry (with a controllable clock);
zombie commits rejected; committed offsets survive a broker restart; internal topic
hidden and protected. End to end: two members split a topic and read every record
exactly once; a restarted consumer resumes from its group's commit; separate groups
each get everything; `close()` moves partitions immediately; a crashed member's
partitions move only after its session times out.

CLI (6): create, list, produce from arguments and stdin, consume, consume with a group
and resume, lag, offsets, error exit codes, unreachable broker.

Chaos (12): the checker detects each kind of violation from hand-made data (who checks
the checker?); the proxy forwards, black-holes when cut, recovers when healed and
delays when slowed; and every fault once against three real broker processes.

## 12. Planned

Metrics and benchmarks (Phase 6), Docker packaging (Phase 7). Raft snapshots and log
compaction, so the Raft log stops growing and restarts stop replaying from the start.
Per-partition leaders, to spread write load across brokers. Idempotent producers, to
remove duplicates from retries.
