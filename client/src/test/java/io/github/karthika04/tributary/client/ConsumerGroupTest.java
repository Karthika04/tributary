package io.github.karthika04.tributary.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.broker.BrokerConfig;
import io.github.karthika04.tributary.broker.BrokerServer;
import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.TopicPartition;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsumerGroupTest {

    private static final Duration SHORT = Duration.ofMillis(50);

    @TempDir
    Path dataDir;

    private BrokerServer startBroker() throws IOException {
        return BrokerServer.start(BrokerConfig.defaults(dataDir).withPort(0).withGroupCheckIntervalMs(50));
    }

    private static ClientConfig clientConfig(BrokerServer broker) {
        return ClientConfig.of("localhost", broker.port());
    }

    /** Fast heartbeats and a short session so rebalances happen within a test's lifetime. */
    private static ConsumerConfig groupConfig(BrokerServer broker, String group) {
        return ConsumerConfig.of(clientConfig(broker)).withGroup(group).withSession(1_500, 100);
    }

    private static void createTopic(BrokerServer broker, String topic, int partitions) {
        try (TributaryClient admin = new TributaryClient(clientConfig(broker))) {
            admin.createTopic(topic, partitions);
        }
    }

    private static void produce(BrokerServer broker, String topic, int count) {
        try (Producer producer = new Producer(ProducerConfig.of(clientConfig(broker)))) {
            for (int i = 0; i < count; i++) {
                producer.send(topic, ("key-" + i).getBytes(UTF_8), ("message-" + i).getBytes(UTF_8));
            }
        }
    }

    /** Polls every consumer in turn until the condition holds (all in this one thread). */
    private static void pollUntil(BooleanSupplier condition, List<ConsumerRecord> sink, Consumer... consumers) {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!condition.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < deadline, "timed out waiting for the group");
            for (Consumer c : consumers) {
                sink.addAll(c.poll(SHORT));
            }
        }
    }

    private static Set<String> values(List<ConsumerRecord> records) {
        Set<String> values = new HashSet<>();
        records.forEach(r -> values.add(r.valueAsString()));
        return values;
    }

    @Test
    void twoMembersSplitThePartitionsAndReadEverythingOnce() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "orders", 4);
            try (Consumer a = new Consumer(groupConfig(broker, "billing"));
                 Consumer b = new Consumer(groupConfig(broker, "billing"))) {
                a.subscribe("orders");
                b.subscribe("orders");
                List<ConsumerRecord> ignored = new ArrayList<>();
                pollUntil(() -> a.assignment().size() == 2 && b.assignment().size() == 2, ignored, a, b);

                Set<TopicPartition> overlap = new HashSet<>(a.assignment());
                overlap.retainAll(b.assignment());
                assertTrue(overlap.isEmpty(), "both members own " + overlap);

                produce(broker, "orders", 400);
                List<ConsumerRecord> fromA = new ArrayList<>();
                List<ConsumerRecord> fromB = new ArrayList<>();
                long deadline = System.currentTimeMillis() + 15_000;
                while (fromA.size() + fromB.size() < 400 && System.currentTimeMillis() < deadline) {
                    fromA.addAll(a.poll(SHORT));
                    fromB.addAll(b.poll(SHORT));
                }

                assertEquals(400, fromA.size() + fromB.size());
                assertFalse(fromA.isEmpty());
                assertFalse(fromB.isEmpty());
                fromA.forEach(r -> assertTrue(a.assignment().contains(new TopicPartition(r.topic(), r.partition()))));
                Set<String> all = values(fromA);
                all.addAll(values(fromB));
                assertEquals(400, all.size()); // nothing read twice, nothing missed
            }
        }
    }

    @Test
    void aRestartedConsumerResumesWhereTheGroupLeftOff() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "events", 1);
            produce(broker, "events", 100);
            TopicPartition tp = new TopicPartition("events", 0);

            List<ConsumerRecord> firstRun = new ArrayList<>();
            try (Consumer first = new Consumer(groupConfig(broker, "reports").withMaxPollRecords(10))) {
                first.subscribe("events");
                pollUntil(() -> firstRun.size() >= 50, firstRun, first);
            } // close() commits the position and leaves the group
            assertEquals(50, firstRun.size());

            try (Consumer second = new Consumer(groupConfig(broker, "reports"))) {
                second.subscribe("events");
                List<ConsumerRecord> secondRun = new ArrayList<>();
                pollUntil(() -> !secondRun.isEmpty(), secondRun, second);

                assertEquals(50, second.committed(tp));
                assertEquals(50, secondRun.get(0).offset()); // not 0: the group remembered
            }
        }
    }

    @Test
    void differentGroupsEachGetEveryRecord() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "orders", 2);
            produce(broker, "orders", 20);
            try (Consumer billing = new Consumer(groupConfig(broker, "billing"));
                 Consumer shipping = new Consumer(groupConfig(broker, "shipping"))) {
                billing.subscribe("orders");
                shipping.subscribe("orders");
                List<ConsumerRecord> billingRecords = new ArrayList<>();
                List<ConsumerRecord> shippingRecords = new ArrayList<>();
                pollUntil(() -> billingRecords.size() >= 20, billingRecords, billing);
                pollUntil(() -> shippingRecords.size() >= 20, shippingRecords, shipping);

                assertEquals(values(billingRecords), values(shippingRecords));
                assertEquals(20, values(billingRecords).size());
            }
        }
    }

    @Test
    void closingAMemberHandsItsPartitionsToTheOthersRightAway() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "orders", 4);
            try (Consumer stays = new Consumer(groupConfig(broker, "billing"))) {
                stays.subscribe("orders");
                Consumer leaves = new Consumer(groupConfig(broker, "billing"));
                leaves.subscribe("orders");
                List<ConsumerRecord> ignored = new ArrayList<>();
                pollUntil(() -> stays.assignment().size() == 2 && leaves.assignment().size() == 2, ignored,
                        stays, leaves);

                long start = System.currentTimeMillis();
                leaves.close(); // sends LeaveGroup
                pollUntil(() -> stays.assignment().size() == 4, ignored, stays);

                long tookMs = System.currentTimeMillis() - start;
                assertTrue(tookMs < 1_500, "took " + tookMs + " ms; should not wait for the session timeout");
            }
        }
    }

    @Test
    void aCrashedMembersPartitionsMoveAfterItsSessionTimesOut() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "orders", 4);
            try (Consumer survivor = new Consumer(groupConfig(broker, "billing"));
                 Consumer crashed = new Consumer(groupConfig(broker, "billing").withAutoCommit(false, 1_000))) {
                survivor.subscribe("orders");
                crashed.subscribe("orders");
                List<ConsumerRecord> ignored = new ArrayList<>();
                pollUntil(() -> survivor.assignment().size() == 2 && crashed.assignment().size() == 2, ignored,
                        survivor, crashed);

                // "crashed" stops polling, so it stops heartbeating, but never says goodbye.
                long start = System.currentTimeMillis();
                pollUntil(() -> survivor.assignment().size() == 4, ignored, survivor);

                long tookMs = System.currentTimeMillis() - start;
                assertTrue(tookMs >= 1_000, "partitions moved after only " + tookMs + " ms");
            }
        }
    }

    @Test
    void aMemberThatLostPartitionsCannotCommitThem() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "orders", 2);
            produce(broker, "orders", 20);
            try (Consumer zombie = new Consumer(groupConfig(broker, "billing").withAutoCommit(false, 1_000))) {
                zombie.subscribe("orders");
                List<ConsumerRecord> read = new ArrayList<>();
                pollUntil(() -> read.size() >= 20, read, zombie); // owns and has read both partitions

                try (Consumer newcomer = new Consumer(groupConfig(broker, "billing"))) {
                    newcomer.subscribe("orders");
                    List<ConsumerRecord> ignored = new ArrayList<>();
                    pollUntil(() -> newcomer.assignment().size() == 1, ignored, newcomer);

                    // The zombie has not polled, so it does not know it lost a partition.
                    Set<TopicPartition> rejected = zombie.commitSync();

                    assertEquals(newcomer.assignment(), rejected);
                    TopicPartition stolen = rejected.iterator().next();
                    assertEquals(-1, newcomer.committed(stolen)); // the zombie's write never landed
                }
            }
        }
    }

    @Test
    void anExtraMemberWithNoPartitionsStillPollsHappily() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "single", 1);
            try (Consumer a = new Consumer(groupConfig(broker, "g"));
                 Consumer b = new Consumer(groupConfig(broker, "g"))) {
                a.subscribe("single");
                b.subscribe("single");
                List<ConsumerRecord> ignored = new ArrayList<>();
                pollUntil(() -> a.assignment().size() + b.assignment().size() == 1
                        && (a.assignment().isEmpty() || b.assignment().isEmpty()), ignored, a, b);

                Consumer idle = a.assignment().isEmpty() ? a : b;
                assertEquals(List.of(), idle.poll(Duration.ofMillis(300)));
            }
        }
    }

    @Test
    void groupOnlyOperationsNeedAGroup() throws IOException {
        try (BrokerServer broker = startBroker()) {
            createTopic(broker, "t", 1);
            try (Consumer plain = new Consumer(ConsumerConfig.of(clientConfig(broker)));
                 Consumer member = new Consumer(groupConfig(broker, "g"))) {
                assertThrows(IllegalStateException.class, plain::commitSync);
                assertThrows(IllegalStateException.class,
                        () -> member.assign(List.of(new TopicPartition("t", 0))));
                TributaryException missing = assertThrows(TributaryException.class, () -> member.subscribe("nope"));
                assertEquals(ErrorCode.UNKNOWN_TOPIC, missing.error());
            }
        }
    }
}
