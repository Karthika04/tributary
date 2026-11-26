# Tributary design

Tributary is a distributed, fault-tolerant message queue in the style of Apache Kafka,
written from scratch in Java. This document explains what it does, how it is built,
and why each major decision was made.

Status: **v2: storage engine, wire protocol, broker, client library and CLI complete.**
Later sections are marked *planned*.

## 1. Goals

- **Durable, ordered streams.** Producers append messages to a topic partition; every
  message gets a unique, increasing offset; consumers read from any offset.
- **Survive crashes.** After a process crash or power loss, no acknowledged message is
  lost or corrupted, and damaged data is detected rather than returned.
- **Survive machine failures** *(planned)*. Each partition is replicated to 3 nodes
  with Raft, so the cluster keeps working when one node dies.
- **High throughput.** Batched writes, append-only files and an O(1) index.
- **Evidence over claims.** Every guarantee above is backed by automated tests,
  including fault-injection ("chaos") tests *(planned)*.

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
| `broker` | TCP server, topics and partitions, long polling, retention | storage, protocol |
| `client` | `TributaryClient`, batching `Producer`, polling `Consumer` | protocol |
| `cli` | The `tributary` command-line tool | client |

Planned: consumer groups (Phase 3), Raft replication (Phase 4).

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

- **correlationId:** chosen by the client and echoed by the broker, so a response can
  be matched to its request. The client checks it and drops the connection on a
  mismatch rather than ever handing back the wrong answer.
- **version:** lets the protocol evolve without breaking old clients. Unknown versions
  get an `UNSUPPORTED_VERSION` error.
- **errors:** 13 error codes, each marked retriable or not, so clients know whether
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
  resets to the oldest (or newest) offset, per `autoOffsetReset`. Positions are
  in memory for now; consumer groups will store them on the broker.

## 8. Testing

94 automated tests across the modules.

Storage (43), including:

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

The storage tests were also checked by deliberately breaking the code (removing the
truncation step, skipping crash recovery) and confirming the right tests fail.

Protocol (18): every message type round-trips with and without compression; cut-off,
padded, wrong-version and lying-compression-size inputs are rejected; murmur2 matches
Kafka's published values; keys spread evenly.

Broker (16), against a real broker on a random port: topic errors; broker timestamps;
compression both ways; a waiting fetch returns as soon as data arrives and gives up on
time; shutdown wakes waiting fetches; data survives a restart; background retention;
50 concurrent clients; malformed requests get an error and the connection keeps
working; garbage and oversized frames close the connection.

Client (12): every record gets a unique, dense offset; keyed records stay on their
partition in order; unknown topics fail the future; the producer keeps retrying while
the broker is down and succeeds once it is back; the consumer reads every partition
with no gaps; latest vs earliest; poll wakes on new data; seek; reset after retention
deletes the position; compression end to end.

CLI (5): create, list, produce from arguments and stdin, consume, offsets, error exit
codes, unreachable broker.

## 9. Planned

Consumer groups with committed offsets (Phase 3), Raft replication with Apache Ratis
(Phase 4), chaos testing (Phase 5), metrics and benchmarks (Phase 6), Docker packaging
(Phase 7).
