# Tributary

[![CI](https://github.com/Karthika04/tributary/actions/workflows/ci.yml/badge.svg)](https://github.com/Karthika04/tributary/actions/workflows/ci.yml)

A distributed, fault-tolerant message queue built from scratch in Java, in the style of
Apache Kafka.

> **Status:** a working replicated message queue: crash-safe storage, a binary TCP
> protocol, a broker with consumer groups, a Java client library, a CLI, and a
> 3-broker cluster that survives losing any broker, with Raft written from scratch.
> Chaos-tested against real broker processes, and fuzzed.

## Try it in two minutes

Requires JDK 21 or newer. The Gradle wrapper downloads everything else.

```bash
./gradlew installDist

# terminal 1: start a broker (Ctrl+C stops it cleanly)
broker/build/install/tributary-broker/bin/tributary-broker --data-dir ./data

# terminal 2: use it
alias tributary=cli/build/install/tributary/bin/tributary
tributary create-topic orders --partitions 3
tributary produce orders --key user-42 "clicked checkout" "paid"
printf 'order-1 shipped\norder-2 shipped\n' | tributary produce orders
tributary offsets orders
tributary consume orders --from-beginning --print-metadata
```

Leave a `tributary consume orders` running and produce from another terminal: messages
appear within milliseconds, thanks to long polling.

Consumer groups share the work and remember their place:

```bash
# run this in two terminals: the 3 partitions are split between them
tributary consume orders --group billing --print-metadata

# how far behind is the group?
tributary lag billing orders
```

### Run a 3-broker cluster and kill the leader

```bash
CLUSTER=1=localhost:9292,2=localhost:9293,3=localhost:9294
export TRIBUTARY_BOOTSTRAP=localhost:9292,localhost:9293,localhost:9294

# three terminals, one broker each
broker/build/install/tributary-broker/bin/tributary-broker --node-id 1 --cluster $CLUSTER --data-dir ./n1
broker/build/install/tributary-broker/bin/tributary-broker --node-id 2 --cluster $CLUSTER --data-dir ./n2
broker/build/install/tributary-broker/bin/tributary-broker --node-id 3 --cluster $CLUSTER --data-dir ./n3

# a fourth terminal (with the same export)
alias tributary=cli/build/install/tributary/bin/tributary
tributary create-topic orders --partitions 1
seq 1 5 | sed 's/^/order-/' | tributary produce orders
```

One broker logs `is the leader`. Press Ctrl+C in its terminal, then keep going:

```bash
seq 6 8 | sed 's/^/order-/' | tributary produce orders   # a new leader takes over in about a second
tributary consume orders --from-beginning --max-messages 8  # all 8 are there
```

Start the stopped broker again and it catches up on everything it missed.

## Features

**Storage engine** (`storage`)
- Append-only, segmented commit log; offsets are sequential and never change
- O(1) reads by offset through a dense, memory-mapped index
- CRC32C checksum on every record: damaged data raises an error, never returns silently
- Crash recovery: half-written records are detected and cut off; every complete record survives
- Retention by age or total size; batches spill across segments automatically

**Replication** (`raft`, `broker`)
- Raft written from scratch (paper section 5): randomized elections, the up-to-date
  vote rule, log replication with conflict truncation, current-term commit rule,
  leader no-op, crash-safe term and vote
  ([why our own Raft?](docs/adr/0003-raft-from-scratch-single-group.md))
- Every write (topics, records, committed offsets) is acknowledged only after a
  majority of brokers stored it: no acknowledged write is lost when a broker dies
- Followers redirect clients to the leader; the client fails over across brokers
  on its own
- Tested over a simulated network with crashes and partitions, including a randomized
  chaos test, and with three real brokers whose leader is killed mid-stream

**Broker** (`broker`)
- Custom binary protocol over TCP with request versioning and correlation IDs
  ([why not gRPC?](docs/adr/0001-custom-tcp-protocol.md))
- One virtual thread per connection (Java 21): simple blocking code that scales to
  thousands of clients
- Topics with multiple partitions; data survives restarts
- Long-polling fetch: consumers wait on the broker and wake the instant data arrives
- GZIP compression in both directions; zip-bomb and oversized-frame protection
- Graceful shutdown that never loses an acknowledged write
- **Consumer groups**: members split partitions automatically (range or round-robin),
  rebalance on join, leave or crash (session timeouts), and commit progress to an
  internal `__consumer_offsets` topic; stale members are fenced from committing
  ([design notes](docs/adr/0002-broker-side-group-assignment.md))

**Client library** (`client`)
- `Producer`: async `send()` returning a `CompletableFuture`, per-partition batching
  with `lingerMs`/`batchBytes`, retries with exponential backoff, ordered per partition
- Key-based partitioning with Kafka's murmur2 hash (verified against Kafka's test values)
- `Consumer`: join a group or assign partitions, `poll()`, auto or manual commit,
  `seek()`, earliest/latest reset

```java
ClientConfig broker = ClientConfig.of("localhost", 9292);

try (Producer producer = new Producer(ProducerConfig.of(broker))) {
    producer.send("orders", "user-42".getBytes(), "clicked checkout".getBytes())
            .thenAccept(meta -> System.out.println("stored at offset " + meta.offset()));
}

try (Consumer consumer = new Consumer(ConsumerConfig.of(broker).withGroup("billing"))) {
    consumer.subscribe("orders");
    for (ConsumerRecord r : consumer.poll(Duration.ofSeconds(1))) {
        System.out.println(r.partition() + "@" + r.offset() + ": " + r.valueAsString());
    }
}
```

How and why it all works is explained in [docs/DESIGN.md](docs/DESIGN.md).

## Chaos tested

Three brokers run as real processes while producers write, and a harness breaks
them on purpose: `kill -9` of the leader, a follower or all three; cutting the leader
or a follower off the network; freezing the leader with `SIGSTOP`; slow links. A
checker then compares every acknowledged write with what was stored.
**500 rounds, 2.46 million acknowledged writes, none lost**, no ordering violations,
and the brokers' Raft logs agreed every time.

| Fault | Rounds | Lost writes | Writes stall for (median) |
|---|---:|---:|---:|
| Leader killed (`kill -9`) | 72 | 0 | 1.5 s |
| Follower killed | 72 | 0 | 13 ms |
| All three killed at once | 72 | 0 | 4.5 s (restart) |
| Leader cut off from the others | 71 | 0 | 0.9 s |
| Follower cut off | 71 | 0 | 11 ms |
| Leader frozen (`SIGSTOP`) | 71 | 0 | 2.4 s |
| 50 to 250 ms network delay | 71 | 0 | 318 ms |

The chaos tests found two real Raft problems, now fixed (CheckQuorum and PreVote), and
fuzzing the decoders found three bugs, including a 150-byte message that made the
broker allocate 2 GB. Details, outliers and the full results:
[DESIGN.md section 10](docs/DESIGN.md#10-chaos-testing-and-fuzzing).

```bash
./gradlew :chaos:installDist
chaos/build/install/chaos-runner/bin/chaos-runner --clusters 5 --rounds 7
```

## Benchmarked

On a MacBook Pro (M3 Pro, 12 cores), with `tributary-perf` (latency percentiles from
HdrHistogram, corrected for coordinated omission) and JMH:

| | Single broker | 3-broker cluster (Raft) |
|---|---:|---:|
| Throughput, 100 B records | 3.3 M records/s | 513 K records/s |
| Throughput, 10 KB records | 605 MB/s | 62 MB/s |
| Produce latency at 10,000 records/s, p50 / p99 | 3.1 / 6.4 ms | 13.2 / 20.8 ms |
| End-to-end latency at 1,000 records/s, p99 | 7.0 ms | 28.9 ms |

The cluster acknowledges a write only once a majority has forced it to disk, and an
`fsync` takes about 4 to 6 ms on this Mac (measured with JMH): that is most of its
latency, a deliberate trade of speed for durability.

The first benchmark runs found four problems, now fixed: a producer bug that let
batches grow until the broker refused them; ~200 ms latency spikes from the consumer
waiting on one partition at a time (fixed with a multi-partition fetch); cluster
throughput capped by one round trip per small batch (2x to 2.4x faster with 1 MB
batches); and, found with Java Flight Recorder, the Raft leader copying every command
twice over while encoding it (2.5x to 3x faster).

```bash
./gradlew :bench:installDist
bench/build/install/tributary-perf/bin/tributary-perf suite     # single broker and 3-broker cluster
./gradlew :bench:jmh                                            # micro-benchmarks
```

All results and the before/after numbers: [DESIGN.md section 11](docs/DESIGN.md#11-performance).

## Observable

```bash
tributary-broker --metrics-port 9404          # Prometheus metrics on http://localhost:9404/metrics
TRIBUTARY_LOG_FORMAT=JSON tributary-broker    # one JSON object per log line
```

Request rate, errors and p99 latency by API, records and bytes by topic, **consumer
lag** by group and partition, Raft leader, term and commit progress, and JVM health,
through Micrometer; a ready-made Grafana dashboard (`deploy/grafana/`); and logs that
carry each request's client and correlation ID. Details:
[DESIGN.md section 12](docs/DESIGN.md#12-observability).

## Build and test

```bash
./gradlew build
```

Compiles every module and runs 175 tests, including every chaos fault once against
real broker processes, fuzzing of every decoder, Raft safety under random crashes
and partitions, a 3-broker cluster losing its leader in the middle of a stream of
writes, and end-to-end tests against a real broker: long polling, restarts, retries
while the broker is down, 50 concurrent clients, consumer-group rebalances and crashed
members, zombie fencing, crash recovery and malformed network input. Reports land in each module's
`build/reports/tests/test/index.html`.

## Project layout

```
storage/    segmented, crash-safe commit log            (JDK only)
protocol/   wire format, framing, compression, hashing  (JDK only)
raft/       Raft consensus: elections, replication, safety  (uses storage)
broker/     TCP server, topics, long polling, retention, consumer groups, cluster mode
client/     TributaryClient, Producer, Consumer
cli/        the `tributary` command-line tool
chaos/      chaos testing: broker processes, fault-injecting proxy, checker
bench/      tributary-perf load generator and JMH micro-benchmarks
deploy/     Prometheus configuration and Grafana dashboard
docs/       design document and architecture decision records
```

## Roadmap

- [x] Storage engine: segments, index, crash recovery, retention
- [x] Broker, binary protocol, Java client library, CLI
- [x] Consumer groups with committed offsets
- [x] 3-broker replication with Raft, written from scratch
- [ ] Raft snapshots and log compaction
- [x] Chaos testing: kill -9, partitions, frozen processes, slow links; fuzzing
- [x] Benchmarks: load generator with latency percentiles, JMH, profiling-driven fixes
- [x] Metrics (Micrometer, Prometheus), a Grafana dashboard, JSON logs with request IDs
- [ ] Docker Compose cluster

## License

MIT
