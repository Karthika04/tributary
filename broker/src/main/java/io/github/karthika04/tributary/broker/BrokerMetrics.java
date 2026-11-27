package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.ApiKey;
import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.TopicPartition;
import io.github.karthika04.tributary.raft.RaftNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The broker's metrics, recorded through Micrometer's facade (the {@link MeterRegistry}
 * API), so the rest of the broker never sees how they are exported. {@link BrokerMain}
 * exports them in Prometheus' format on {@code /metrics}.
 *
 * <p>Names use dots; the Prometheus registry turns {@code tributary.produce.records}
 * into {@code tributary_produce_records_total}. The metrics:
 * <ul>
 *   <li>{@code tributary.request.latency} (timer, with a histogram for p99s in
 *       Grafana), tagged by API and error code: request rate, error rate and latency.</li>
 *   <li>{@code tributary.produce.records}, {@code .bytes}; {@code tributary.fetch.records},
 *       {@code .bytes} (counters, by topic).</li>
 *   <li>{@code tributary.connections} and {@code tributary.partition.end.offset} (gauges).</li>
 *   <li>{@code tributary.consumer.lag} (gauge, by group, topic and partition): how many
 *       records a group has not committed yet. The number operators watch most.</li>
 *   <li>Raft, in a cluster: {@code tributary.raft.term}, {@code .commit.index},
 *       {@code .last.applied}, {@code .leader} (1 on the leader), and
 *       {@code tributary.raft.leader.changes} (counter).</li>
 *   <li>The JVM: memory, garbage collection, threads, CPU, uptime.</li>
 * </ul>
 */
final class BrokerMetrics implements AutoCloseable {

    private final MeterRegistry registry;
    private final JvmGcMetrics gcMetrics = new JvmGcMetrics();
    private final Map<String, Timer> requestTimers = new ConcurrentHashMap<>();
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Set<String> partitionGauges = ConcurrentHashMap.newKeySet();
    private final Set<String> lagGauges = ConcurrentHashMap.newKeySet();
    private Counter leaderChanges;
    private volatile String lastLeader;

    BrokerMetrics(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry);
        new JvmMemoryMetrics().bindTo(registry);
        gcMetrics.bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ClassLoaderMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
    }

    MeterRegistry registry() {
        return registry;
    }

    /** One request handled, how long it took, and how it ended. */
    void request(ApiKey api, ErrorCode error, long nanos) {
        String key = api.name() + "/" + error.name();
        requestTimers.computeIfAbsent(key, k -> Timer.builder("tributary.request.latency")
                .description("Time to handle a request, from decoding it to having the response ready")
                .tag("api", lower(api.name()))
                .tag("error", lower(error.name()))
                .publishPercentileHistogram()
                .register(registry))
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    void produced(String topic, int records, long bytes) {
        counter("tributary.produce.records", topic, "Records appended").increment(records);
        counter("tributary.produce.bytes", topic, "Bytes of keys and values appended").increment(bytes);
    }

    void fetched(String topic, int records, long bytes) {
        counter("tributary.fetch.records", topic, "Records sent to consumers").increment(records);
        counter("tributary.fetch.bytes", topic, "Bytes of keys and values sent to consumers").increment(bytes);
    }

    private Counter counter(String name, String topic, String description) {
        return counters.computeIfAbsent(name + "/" + topic, k -> Counter.builder(name)
                .description(description).tag("topic", topic).register(registry));
    }

    void connections(Collection<?> openSockets) {
        Gauge.builder("tributary.connections", openSockets, Collection::size)
                .description("Open client and broker connections").strongReference(true).register(registry);
    }

    /** Adds gauges for partitions that do not have them yet. Cheap to call repeatedly. */
    void refreshPartitions(TopicManager topics) {
        for (Response.TopicMetadata topic : topics.metadata()) {
            for (int p = 0; p < topic.partitions(); p++) {
                String key = topic.name() + "/" + p;
                if (partitionGauges.add(key)) {
                    Partition partition = topics.partition(topic.name(), p);
                    Gauge.builder("tributary.partition.end.offset", partition, Partition::endOffset)
                            .description("The offset the next record in the partition will get")
                            .tag("topic", topic.name()).tag("partition", Integer.toString(p))
                            .strongReference(true).register(registry);
                }
            }
        }
    }

    /**
     * Adds a lag gauge for every (group, partition) that has committed an offset. The
     * gauge computes lag live when scraped: the partition's end offset minus the
     * group's committed offset.
     */
    void refreshLag(TopicManager topics, OffsetStore offsets) {
        for (Map.Entry<String, Set<TopicPartition>> group : offsets.committedPartitions().entrySet()) {
            for (TopicPartition tp : group.getValue()) {
                String key = group.getKey() + "/" + tp;
                if (!lagGauges.add(key)) {
                    continue;
                }
                String groupId = group.getKey();
                Gauge.builder("tributary.consumer.lag", tp, t -> lag(topics, offsets, groupId, t))
                        .description("Records in the partition not yet committed by the group")
                        .tag("group", groupId).tag("topic", tp.topic()).tag("partition", Integer.toString(tp.partition()))
                        .strongReference(true).register(registry);
            }
        }
    }

    private static double lag(TopicManager topics, OffsetStore offsets, String group, TopicPartition tp) {
        try {
            long committed = offsets.committed(group, tp);
            long end = topics.partition(tp.topic(), tp.partition()).endOffset();
            return committed < 0 ? end : Math.max(0, end - committed);
        } catch (RuntimeException e) {
            return Double.NaN; // the topic is gone, or the broker is closing
        }
    }

    /** Raft gauges, in cluster mode. */
    void raft(RaftNode raft) {
        Gauge.builder("tributary.raft.term", raft, RaftNode::currentTerm)
                .description("The Raft term this broker is in").strongReference(true).register(registry);
        Gauge.builder("tributary.raft.commit.index", raft, RaftNode::commitIndex)
                .description("Highest Raft log index known to be committed").strongReference(true).register(registry);
        Gauge.builder("tributary.raft.last.applied", raft, RaftNode::lastApplied)
                .description("Highest Raft log index applied to this broker's topics").strongReference(true)
                .register(registry);
        Gauge.builder("tributary.raft.leader", raft, r -> r.isLeader() ? 1 : 0)
                .description("1 on the Raft leader, 0 elsewhere").strongReference(true).register(registry);
        leaderChanges = Counter.builder("tributary.raft.leader.changes")
                .description("Times this broker saw the cluster's leader change").register(registry);
        lastLeader = raft.leaderId();
    }

    /** Counts a leader change if the leader differs from the last check (called on a timer). */
    void sampleLeader(RaftNode raft) {
        String leader = raft.leaderId();
        if (leader != null && !leader.equals(lastLeader)) {
            if (lastLeader != null) {
                leaderChanges.increment();
            }
            lastLeader = leader;
        }
    }

    private static String lower(String s) {
        return s.toLowerCase(Locale.ROOT);
    }

    @Override
    public void close() {
        gcMetrics.close();
        registry.close();
    }
}
