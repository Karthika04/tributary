# Tributary

[![CI](https://github.com/Karthika04/tributary/actions/workflows/ci.yml/badge.svg)](https://github.com/Karthika04/tributary/actions/workflows/ci.yml)

A distributed, fault-tolerant message queue built from scratch in Java, in the style of
Apache Kafka.

> **Status:** a working single-node message queue: crash-safe storage, a binary TCP
> protocol, a broker, a Java client library and a CLI. Consumer groups and Raft
> replication are in progress.

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

## Features

**Storage engine** (`storage`)
- Append-only, segmented commit log; offsets are sequential and never change
- O(1) reads by offset through a dense, memory-mapped index
- CRC32C checksum on every record: damaged data raises an error, never returns silently
- Crash recovery: half-written records are detected and cut off; every complete record survives
- Retention by age or total size; batches spill across segments automatically

**Broker** (`broker`)
- Custom binary protocol over TCP with request versioning and correlation IDs
  ([why not gRPC?](docs/adr/0001-custom-tcp-protocol.md))
- One virtual thread per connection (Java 21): simple blocking code that scales to
  thousands of clients
- Topics with multiple partitions; data survives restarts
- Long-polling fetch: consumers wait on the broker and wake the instant data arrives
- GZIP compression in both directions; zip-bomb and oversized-frame protection
- Graceful shutdown that never loses an acknowledged write

**Client library** (`client`)
- `Producer`: async `send()` returning a `CompletableFuture`, per-partition batching
  with `lingerMs`/`batchBytes`, retries with exponential backoff, ordered per partition
- Key-based partitioning with Kafka's murmur2 hash (verified against Kafka's test values)
- `Consumer`: subscribe or assign partitions, `poll()`, `seek()`, earliest/latest reset

```java
ClientConfig broker = ClientConfig.of("localhost", 9292);

try (Producer producer = new Producer(ProducerConfig.of(broker))) {
    producer.send("orders", "user-42".getBytes(), "clicked checkout".getBytes())
            .thenAccept(meta -> System.out.println("stored at offset " + meta.offset()));
}

try (Consumer consumer = new Consumer(ConsumerConfig.of(broker))) {
    consumer.subscribe("orders");
    for (ConsumerRecord r : consumer.poll(Duration.ofSeconds(1))) {
        System.out.println(r.partition() + "@" + r.offset() + ": " + r.valueAsString());
    }
}
```

How and why it all works is explained in [docs/DESIGN.md](docs/DESIGN.md).

## Build and test

```bash
./gradlew build
```

Compiles every module and runs 94 tests, including end-to-end tests against a real
broker: long polling, restarts, retries while the broker is down, 50 concurrent
clients, crash recovery and malformed network input. Reports land in each module's
`build/reports/tests/test/index.html`.

## Project layout

```
storage/    segmented, crash-safe commit log            (JDK only)
protocol/   wire format, framing, compression, hashing  (JDK only)
broker/     TCP server, topics, long polling, retention
client/     TributaryClient, Producer, Consumer
cli/        the `tributary` command-line tool
docs/       design document and architecture decision records
```

## Roadmap

- [x] Storage engine: segments, index, crash recovery, retention
- [x] Broker, binary protocol, Java client library, CLI
- [ ] Consumer groups with committed offsets
- [ ] 3-node replication with Raft (Apache Ratis)
- [ ] Chaos testing: node kills, network partitions
- [ ] Metrics (Prometheus, Grafana) and benchmarks
- [ ] Docker Compose cluster

## License

MIT
