package io.github.karthika04.tributary.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.broker.BrokerConfig;
import io.github.karthika04.tributary.broker.BrokerServer;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.Partitioning;
import io.github.karthika04.tributary.protocol.TopicPartition;
import io.github.karthika04.tributary.storage.LogConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProducerConsumerTest {

    @TempDir
    Path dataDir;

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private BrokerServer startBroker() throws IOException {
        return BrokerServer.start(BrokerConfig.defaults(dataDir).withPort(0));
    }

    private static ClientConfig clientConfig(BrokerServer broker) {
        return ClientConfig.of("localhost", broker.port());
    }

    private static void createTopic(BrokerServer broker, String topic, int partitions) {
        try (TributaryClient admin = new TributaryClient(clientConfig(broker))) {
            admin.createTopic(topic, partitions);
        }
    }

    private static List<ConsumerRecord> pollUntil(Consumer consumer, int count, Duration timeout) {
        List<ConsumerRecord> all = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (all.size() < count && System.nanoTime() < deadline) {
            all.addAll(consumer.poll(Duration.ofMillis(200)));
        }
        return all;
    }

    // ---- producer ----------------------------------------------------------------

    @Test
    void producerReturnsAnOffsetForEveryRecord() throws Exception {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "events", 3);
            List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
            try (Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
                for (int i = 0; i < 1000; i++) {
                    futures.add(producer.send("events", null, bytes("event-" + i)));
                }
                producer.flush();
                for (CompletableFuture<RecordMetadata> f : futures) {
                    assertTrue(f.isDone(), "flush returned before every record had a result");
                }
            }
            // Every (partition, offset) pair is used exactly once, and offsets are dense.
            Map<Integer, Set<Long>> offsetsByPartition = new HashMap<>();
            for (CompletableFuture<RecordMetadata> f : futures) {
                RecordMetadata meta = f.get();
                assertTrue(offsetsByPartition.computeIfAbsent(meta.partition(), p -> new HashSet<>())
                        .add(meta.offset()), "duplicate " + meta);
            }
            try (TributaryClient client = new TributaryClient(clientConfig(broker))) {
                long total = 0;
                for (Map.Entry<Integer, Set<Long>> e : offsetsByPartition.entrySet()) {
                    long end = client.listOffsets("events", e.getKey()).endOffset();
                    assertEquals(end, e.getValue().size());
                    total += end;
                }
                assertEquals(1000, total);
            }
        }
    }

    @Test
    void keyedRecordsGoToTheirKeysPartitionInOrder() throws Exception {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "orders", 4);
            int expectedPartition = Partitioning.partitionForKey(bytes("user-42"), 4);
            List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
            try (Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
                for (int i = 0; i < 50; i++) {
                    futures.add(producer.send("orders", bytes("user-42"), bytes("step-" + i)));
                }
            }
            long previous = -1;
            for (CompletableFuture<RecordMetadata> f : futures) {
                RecordMetadata meta = f.get();
                assertEquals(expectedPartition, meta.partition());
                assertTrue(meta.offset() > previous, "out of order");
                previous = meta.offset();
            }
        }
    }

    @Test
    void sendingToAMissingTopicFailsTheFuture() throws Exception {
        try (BrokerServer broker = startBroker();
             Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
            CompletableFuture<RecordMetadata> f = producer.send("nope", null, bytes("x"));

            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
            assertEquals(ErrorCode.UNKNOWN_TOPIC, ((TributaryException) e.getCause()).error());
        }
    }

    @Test
    void producerRetriesUntilTheBrokerIsBack() throws Exception {
        BrokerServer first = startBroker();
        int port = first.port();
        createTopic(first, "t", 1);
        ProducerConfig config = ProducerConfig.of(ClientConfig.of("localhost", port)).withRetries(20, 50);
        try (Producer producer = new Producer(config)) {
            producer.send("t", 0, null, bytes("before")).get(5, TimeUnit.SECONDS);
            first.close(); // the broker goes away...

            CompletableFuture<RecordMetadata> during = producer.send("t", 0, null, bytes("during outage"));
            Thread.sleep(300);
            assertTrue(!during.isDone(), "should still be retrying");

            try (BrokerServer second = BrokerServer.start(BrokerConfig.defaults(dataDir).withPort(port))) {
                assertEquals(port, second.port());
                RecordMetadata meta = during.get(10, TimeUnit.SECONDS); // ...and comes back
                assertEquals(1, meta.offset());
            }
        }
    }

    @Test
    void sendAfterCloseIsRejected() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "t", 1);
            Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)));
            producer.close();
            assertThrows(IllegalStateException.class, () -> producer.send("t", 0, null, bytes("late")));
        }
    }

    // ---- consumer ----------------------------------------------------------------

    @Test
    void consumerReadsEverythingFromEveryPartition() throws Exception {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "events", 3);
            try (Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
                for (int i = 0; i < 300; i++) {
                    producer.send("events", bytes("key-" + (i % 10)), bytes("event-" + i));
                }
            }
            try (Consumer consumer = new Consumer(ConsumerConfig.of(clientConfig(broker)).withMaxPollRecords(50))) {
                consumer.subscribe("events");
                List<ConsumerRecord> records = pollUntil(consumer, 300, Duration.ofSeconds(10));

                assertEquals(300, records.size());
                Set<String> values = new HashSet<>();
                Map<Integer, Long> lastOffset = new HashMap<>();
                for (ConsumerRecord r : records) {
                    values.add(r.valueAsString());
                    long previous = lastOffset.getOrDefault(r.partition(), -1L);
                    assertEquals(previous + 1, r.offset(), "gap or reorder in partition " + r.partition());
                    lastOffset.put(r.partition(), r.offset());
                }
                assertEquals(300, values.size());
                assertEquals(3, lastOffset.size());
            }
        }
    }

    @Test
    void latestOnlySeesRecordsWrittenAfterwards() throws Exception {
        try (BrokerServer broker = startBroker();
             Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
            createTopic(broker, "t", 1);
            producer.send("t", null, bytes("old")).get();
            try (Consumer consumer = new Consumer(ConsumerConfig.of(clientConfig(broker))
                    .withAutoOffsetReset(ConsumerConfig.AutoOffsetReset.LATEST))) {
                consumer.subscribe("t");
                assertEquals(1, consumer.position(new TopicPartition("t", 0)));

                producer.send("t", null, bytes("new")).get();
                List<ConsumerRecord> records = pollUntil(consumer, 1, Duration.ofSeconds(5));

                assertEquals(1, records.size());
                assertEquals("new", records.get(0).valueAsString());
            }
        }
    }

    @Test
    void pollWakesUpAsSoonAsARecordArrives() throws Exception {
        try (BrokerServer broker = startBroker();
             Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
            createTopic(broker, "t", 1);
            try (Consumer consumer = new Consumer(ConsumerConfig.of(clientConfig(broker)).withFetchMaxWaitMs(5_000))) {
                consumer.subscribe("t");
                consumer.position(new TopicPartition("t", 0));
                CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException ignored) {
                        return;
                    }
                    producer.send("t", null, bytes("ping"));
                });

                long start = System.nanoTime();
                List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(10));
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

                assertEquals(1, records.size());
                assertTrue(elapsedMs < 3_000, "poll took " + elapsedMs + " ms");
            }
        }
    }

    @Test
    void seekMovesThePosition() throws Exception {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "t", 1);
            try (Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
                for (int i = 0; i < 10; i++) {
                    producer.send("t", null, bytes("m" + i));
                }
            }
            TopicPartition tp = new TopicPartition("t", 0);
            try (Consumer consumer = new Consumer(ConsumerConfig.of(clientConfig(broker)))) {
                consumer.assign(List.of(tp));
                consumer.seek(tp, 7);
                assertEquals("m7", consumer.poll(Duration.ofSeconds(1)).get(0).valueAsString());

                consumer.seekToEnd(List.of(tp));
                assertEquals(10, consumer.position(tp));
                assertEquals(0, consumer.poll(Duration.ofMillis(100)).size());

                consumer.seekToBeginning(List.of(tp));
                assertEquals(0, consumer.position(tp));
                assertThrows(IllegalStateException.class, () -> consumer.seek(new TopicPartition("t", 5), 0));
            }
        }
    }

    @Test
    void aPositionDeletedByRetentionResetsToTheOldestRecord() throws Exception {
        LogConfig small = LogConfig.defaults().withMaxRecordBytes(512).withSegmentBytes(1024).withRetentionBytes(2048);
        BrokerConfig config = BrokerConfig.defaults(dataDir).withPort(0).withLogConfig(small)
                .withRetentionCheckIntervalMs(50);
        try (BrokerServer broker = BrokerServer.start(config)) {
            createTopic(broker, "t", 1);
            TopicPartition tp = new TopicPartition("t", 0);
            try (Consumer consumer = new Consumer(ConsumerConfig.of(clientConfig(broker)))) {
                consumer.assign(List.of(tp));
                consumer.seek(tp, 0);
                try (Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
                    for (int i = 0; i < 200; i++) {
                        producer.send("t", 0, null, bytes("message number " + i));
                    }
                }
                try (TributaryClient admin = new TributaryClient(clientConfig(broker))) {
                    long deadline = System.currentTimeMillis() + 5_000;
                    while (admin.listOffsets("t", 0).startOffset() == 0 && System.currentTimeMillis() < deadline) {
                        Thread.sleep(20);
                    }
                    assertEquals(200, admin.listOffsets("t", 0).endOffset()); // every record was stored
                    long start = admin.listOffsets("t", 0).startOffset();
                    assertTrue(start > 0, "retention never ran");

                    List<ConsumerRecord> records = pollUntil(consumer, 1, Duration.ofSeconds(5));
                    assertTrue(records.get(0).offset() >= start, "read a deleted offset");
                }
            }
        }
    }

    @Test
    void compressedEndToEnd() throws Exception {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "logs", 1);
            ProducerConfig producerConfig = ProducerConfig.of(clientConfig(broker)).withCompression(Compression.GZIP);
            try (Producer producer = new Producer(producerConfig)) {
                for (int i = 0; i < 100; i++) {
                    producer.send("logs", null, bytes("{\"level\":\"INFO\",\"n\":" + i + "}"));
                }
            }
            ConsumerConfig consumerConfig = ConsumerConfig.of(clientConfig(broker)).withFetchCompression(Compression.GZIP);
            try (Consumer consumer = new Consumer(consumerConfig)) {
                consumer.subscribe("logs");
                List<ConsumerRecord> records = pollUntil(consumer, 100, Duration.ofSeconds(5));
                assertEquals(100, records.size());
                assertEquals("{\"level\":\"INFO\",\"n\":99}", records.get(99).valueAsString());
            }
        }
    }

    @Test
    void parsesBrokerAddresses() {
        assertEquals(List.of("broker.local:7000"), ClientConfig.parse("broker.local:7000").brokers());
        assertEquals(List.of("a:1", "b:2", "c:3"), ClientConfig.parse("a:1, b:2,c:3").brokers());
        assertThrows(IllegalArgumentException.class, () -> ClientConfig.parse("no-port"));
        assertThrows(IllegalArgumentException.class, () -> ClientConfig.parse("host:"));
        assertThrows(IllegalArgumentException.class, () -> ClientConfig.parse("host:99999"));
        assertThrows(IllegalArgumentException.class, () -> ClientConfig.parse(""));
    }
}
