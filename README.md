# Tributary

[![CI](https://github.com/Karthika04/tributary/actions/workflows/ci.yml/badge.svg)](https://github.com/Karthika04/tributary/actions/workflows/ci.yml)

A distributed, fault-tolerant message queue built from scratch in Java, in the style of
Apache Kafka.

> **Status:** the storage engine (an append-only, segmented commit log with crash
> recovery) is complete and tested. The networked broker, consumer groups and Raft
> replication are in progress.

## What it does today

The `storage` module is the engine every partition runs on:

- **Append-only, segmented log.** Records get sequential offsets that never change.
  Old data lives in read-only segment files; deleting it is deleting files.
- **O(1) reads by offset** through a dense, memory-mapped index.
- **Batched writes:** a whole batch costs one write call.
- **Checksums on every record** (CRC32C). Damaged data raises an error instead of
  being returned.
- **Crash recovery.** After a crash, half-written records are detected and cut off;
  every complete record survives.
- **Retention** by age or total size.
- **Thread-safe:** many concurrent readers, one writer at a time.

```java
try (Log log = Log.open(Path.of("data/orders-0"), LogConfig.defaults())) {
    long offset = log.append("user-42".getBytes(), "clicked checkout".getBytes());
    Record record = log.read(offset);
    List<Record> next100 = log.read(offset, 100);
}
```

How and why it works is explained in [docs/DESIGN.md](docs/DESIGN.md).

## Build and test

Requires JDK 21 or newer. Nothing else: the Gradle wrapper downloads Gradle itself.

```bash
./gradlew build
```

This compiles everything and runs the test suite. Reports:

- Tests: `storage/build/reports/tests/test/index.html`
- Coverage: `storage/build/reports/jacoco/test/html/index.html`

## Project layout

```
storage/   append-only segmented log (done)
docs/      design document and architecture decision records
```

Coming next: `proto` (gRPC API), `broker`, `client`, `cli`.

## Roadmap

- [x] Storage engine: segments, index, crash recovery, retention
- [ ] gRPC broker, Java client library, CLI
- [ ] Consumer groups with committed offsets
- [ ] 3-node replication with Raft (Apache Ratis)
- [ ] Chaos testing: node kills, network partitions
- [ ] Metrics (Prometheus, Grafana) and benchmarks
- [ ] Docker Compose cluster

## License

MIT
