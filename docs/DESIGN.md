# Tributary design

Tributary is a distributed, fault-tolerant message queue in the style of Apache Kafka,
written from scratch in Java. This document explains what it does, how it is built,
and why each major decision was made.

Status: **v1, storage engine complete.** Later sections are marked *planned*.

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
- Kafka wire-protocol compatibility. Tributary uses its own gRPC API.
- Multi-datacenter replication.

## 3. Architecture

```
                 +-------------------------------------------+
   producers --> |  Broker (gRPC API)            [planned]   |
   consumers <-- |    topics -> partitions -> Log            |
                 |    consumer groups, offsets  [planned]    |
                 |    Raft replication          [planned]    |
                 +---------------------+---------------------+
                                       |
                 +---------------------v---------------------+
                 |  storage module (this release)            |
                 |  Log -> Segments -> Store (.log)          |
                 |                    -> Index (.index)      |
                 +-------------------------------------------+
```

Modules: `storage` (done), `proto`, `broker`, `client`, `cli` (planned).

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

1. Validate the whole batch first (record size, batch size), so a batch is accepted
   or rejected as a unit.
2. If the batch does not fit in the active segment, **roll**: flush and seal the
   active segment and create a new one.
3. Encode the whole batch into one buffer and write it with **one** write call.
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

## 5. Testing (storage)

42 automated tests, including:

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

The tests were also checked by deliberately breaking the code (removing the
truncation step, skipping crash recovery) and confirming the right tests fail.

## 6. Planned

See the project tracker for the full plan: gRPC broker and client (Phase 2), consumer
groups (Phase 3), Raft replication with Apache Ratis (Phase 4), chaos testing
(Phase 5), metrics and benchmarks (Phase 6), packaging (Phase 7).
