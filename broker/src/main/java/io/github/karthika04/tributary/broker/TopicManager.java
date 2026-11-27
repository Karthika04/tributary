package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.TopicPartition;
import io.github.karthika04.tributary.storage.Log;
import io.github.karthika04.tributary.storage.LogConfig;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * All topics on this broker. On disk:
 *
 * <pre>
 * dataDir/
 *   orders/      &lt;- topic
 *     0/         &lt;- partition 0, a storage Log
 *     1/
 *   payments/
 *     0/
 * </pre>
 *
 * <p>The folder layout is the source of truth: on startup, every topic folder with
 * partition folders 0..N-1 is loaded back.
 */
final class TopicManager implements Closeable {

    /** Letters, digits, '.', '_' and '-', like Kafka. Safe as a folder name on every OS. */
    private static final Pattern VALID_NAME = Pattern.compile("[A-Za-z0-9._-]{1,200}");
    static final int MAX_PARTITIONS = 1000;

    private final Path dataDir;
    private final LogConfig logConfig;
    private final ConcurrentHashMap<String, List<Partition>> topics = new ConcurrentHashMap<>();

    TopicManager(Path dataDir, LogConfig logConfig) {
        this.dataDir = dataDir;
        this.logConfig = logConfig;
        try {
            Files.createDirectories(dataDir);
            load();
        } catch (IOException e) {
            closeQuietly();
            throw new UncheckedIOException("cannot load topics from " + dataDir, e);
        } catch (RuntimeException e) {
            closeQuietly();
            throw e;
        }
    }

    private void load() throws IOException {
        try (DirectoryStream<Path> topicDirs = Files.newDirectoryStream(dataDir, Files::isDirectory)) {
            for (Path topicDir : topicDirs) {
                String topic = topicDir.getFileName().toString();
                if (!isValidName(topic)) {
                    continue;
                }
                int count = countPartitionDirs(topicDir);
                if (count == 0) {
                    continue;
                }
                List<Partition> partitions = new ArrayList<>(count);
                for (int p = 0; p < count; p++) {
                    partitions.add(openPartition(topic, p));
                }
                topics.put(topic, List.copyOf(partitions));
            }
        }
    }

    /** Counts folders named 0, 1, 2... and insists there are no gaps. */
    private static int countPartitionDirs(Path topicDir) throws IOException {
        int count = 0;
        int max = -1;
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(topicDir, Files::isDirectory)) {
            for (Path dir : dirs) {
                String name = dir.getFileName().toString();
                if (name.matches("\\d{1,4}")) {
                    count++;
                    max = Math.max(max, Integer.parseInt(name));
                }
            }
        }
        if (max != count - 1) {
            throw new IllegalStateException("topic folder " + topicDir + " has partition folders with gaps");
        }
        return count;
    }

    private Partition openPartition(String topic, int partition) {
        Path dir = dataDir.resolve(topic).resolve(Integer.toString(partition));
        return new Partition(new TopicPartition(topic, partition), Log.open(dir, logConfig));
    }

    static boolean isValidName(String name) {
        return name != null && VALID_NAME.matcher(name).matches() && !name.equals(".") && !name.equals("..");
    }

    /** Topics whose names start with "__" belong to the broker itself. */
    static boolean isInternal(String topic) {
        return topic.startsWith("__");
    }

    /** Creating topics is rare, so one lock for all of them keeps it simple. */
    synchronized void createTopic(String topic, int partitionCount) {
        checkNewTopic(topic, partitionCount);
        createTopicUnchecked(topic, partitionCount);
    }

    /** Throws the error that creating this topic would fail with, if any. */
    void checkNewTopic(String topic, int partitionCount) {
        if (!isValidName(topic)) {
            throw new BrokerException(ErrorCode.INVALID_TOPIC, "invalid topic name '" + topic
                    + "': use 1-200 letters, digits, '.', '_' or '-'");
        }
        if (isInternal(topic)) {
            throw new BrokerException(ErrorCode.INVALID_TOPIC, "topic names starting with '__' are reserved");
        }
        if (partitionCount < 1 || partitionCount > MAX_PARTITIONS) {
            throw new BrokerException(ErrorCode.INVALID_REQUEST,
                    "partitions must be between 1 and " + MAX_PARTITIONS + ", got " + partitionCount);
        }
        if (topics.containsKey(topic)) {
            throw new BrokerException(ErrorCode.TOPIC_ALREADY_EXISTS, "topic '" + topic + "' already exists");
        }
    }

    /** Creates a broker-internal topic if it does not exist yet, and returns its partition 0. */
    synchronized Partition internalTopic(String topic) {
        if (!topics.containsKey(topic)) {
            createTopicUnchecked(topic, 1);
        }
        return partition(topic, 0);
    }

    private void createTopicUnchecked(String topic, int partitionCount) {
        List<Partition> partitions = new ArrayList<>(partitionCount);
        try {
            for (int p = 0; p < partitionCount; p++) {
                partitions.add(openPartition(topic, p));
            }
        } catch (RuntimeException e) {
            partitions.forEach(Partition::close);
            throw e;
        }
        topics.put(topic, List.copyOf(partitions));
    }

    Partition partition(String topic, int partition) {
        List<Partition> partitions = topics.get(topic);
        if (partitions == null) {
            throw new BrokerException(ErrorCode.UNKNOWN_TOPIC, "topic '" + topic + "' does not exist");
        }
        if (partition < 0 || partition >= partitions.size()) {
            throw new BrokerException(ErrorCode.UNKNOWN_PARTITION, "topic '" + topic + "' has partitions 0-"
                    + (partitions.size() - 1) + ", not " + partition);
        }
        return partitions.get(partition);
    }

    int partitionCount(String topic) {
        List<Partition> partitions = topics.get(topic);
        if (partitions == null) {
            throw new BrokerException(ErrorCode.UNKNOWN_TOPIC, "topic '" + topic + "' does not exist");
        }
        return partitions.size();
    }

    /** Every topic except the broker's internal ones. */
    List<Response.TopicMetadata> metadata() {
        Map<String, Integer> sorted = new TreeMap<>();
        topics.forEach((name, partitions) -> {
            if (!isInternal(name)) {
                sorted.put(name, partitions.size());
            }
        });
        List<Response.TopicMetadata> result = new ArrayList<>();
        sorted.forEach((name, count) -> result.add(new Response.TopicMetadata(name, count)));
        return result;
    }

    /** @return segments deleted across all partitions */
    int applyRetention() {
        int deleted = 0;
        for (List<Partition> partitions : topics.values()) {
            for (Partition p : partitions) {
                deleted += p.applyRetention();
            }
        }
        return deleted;
    }

    void stopWaiting() {
        topics.values().forEach(partitions -> partitions.forEach(Partition::stopWaiting));
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        for (List<Partition> partitions : topics.values()) {
            for (Partition p : partitions) {
                try {
                    p.close();
                } catch (RuntimeException ignored) {
                    // keep closing the others
                }
            }
        }
        topics.clear();
    }
}
