package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.Codec;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.FetchPosition;
import io.github.karthika04.tributary.protocol.Frames;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.protocol.ProtocolException;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.ResponseEnvelope;
import io.github.karthika04.tributary.protocol.TopicPartition;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A low-level connection to a broker: send a request, get its response.
 *
 * <p>Connects lazily on first use. If the broker cannot be reached, it tries the next
 * address in {@link ClientConfig#brokers()}; if a broker answers {@code NOT_LEADER}, it
 * follows the redirect to the leader. Both are retried with a growing pause, which rides
 * out a leader election. Thread-safe: requests from several threads are sent one at a
 * time. For real applications, {@link Producer} and {@link Consumer} are easier to use.
 *
 * <pre>{@code
 * try (TributaryClient client = new TributaryClient(ClientConfig.of("localhost", 9292))) {
 *     client.createTopic("orders", 3);
 *     long offset = client.produce("orders", 0, List.of(new ProducedRecord(null, bytes)), Compression.NONE);
 * }
 * }</pre>
 */
public final class TributaryClient implements AutoCloseable {

    private static final String LEADER_HINT = "leader is ";
    private static final long MAX_BACKOFF_MS = 1_000;

    private final ClientConfig config;
    private final List<String> addresses;
    private int current;
    private final ReentrantLock lock = new ReentrantLock();
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private int nextCorrelationId;
    private boolean closed;

    public TributaryClient(ClientConfig config) {
        this.config = config;
        this.addresses = new java.util.ArrayList<>(config.brokers());
    }

    /** The broker this client is currently talking to. */
    public String currentBroker() {
        lock.lock();
        try {
            return addresses.get(current);
        } finally {
            lock.unlock();
        }
    }

    public ClientConfig config() {
        return config;
    }

    // ---- typed helpers -----------------------------------------------------------

    public void createTopic(String topic, int partitions) {
        send(new Request.CreateTopic(topic, partitions));
    }

    /** Every topic and its partition count, sorted by name. */
    public Map<String, Integer> listTopics() {
        Response.Metadata metadata = (Response.Metadata) send(new Request.Metadata());
        Map<String, Integer> result = new LinkedHashMap<>();
        for (Response.TopicMetadata t : metadata.topics()) {
            result.put(t.name(), t.partitions());
        }
        return result;
    }

    /** @throws TributaryException with {@link ErrorCode#UNKNOWN_TOPIC} if it does not exist */
    public int partitionCount(String topic) {
        Integer count = listTopics().get(topic);
        if (count == null) {
            throw new TributaryException(ErrorCode.UNKNOWN_TOPIC, "topic '" + topic + "' does not exist");
        }
        return count;
    }

    /** @return the offset given to the first record */
    public long produce(String topic, int partition, List<ProducedRecord> records, Compression compression) {
        return ((Response.Produce) send(new Request.Produce(topic, partition, compression, records))).baseOffset();
    }

    public Response.Fetch fetch(String topic, int partition, long offset, int maxRecords, int maxWaitMs,
                                Compression compression) {
        return (Response.Fetch) send(new Request.Fetch(topic, partition, offset, maxRecords, maxWaitMs,
                compression));
    }

    /** Fetches from many partitions at once; see {@link Request.MultiFetch}. */
    public Response.MultiFetch multiFetch(List<FetchPosition> partitions, int maxRecords, int maxBytes, int maxWaitMs,
                                          Compression compression) {
        return (Response.MultiFetch) send(new Request.MultiFetch(partitions, maxRecords, maxBytes, maxWaitMs,
                compression));
    }

    public Response.ListOffsets listOffsets(String topic, int partition) {
        return (Response.ListOffsets) send(new Request.ListOffsets(topic, partition));
    }

    // ---- consumer groups -----------------------------------------------------------

    public Response.JoinGroup joinGroup(String groupId, String memberId, List<String> topics, int sessionTimeoutMs,
                                        String assignor) {
        return (Response.JoinGroup) send(new Request.JoinGroup(groupId, memberId, topics, sessionTimeoutMs,
                assignor));
    }

    public void heartbeat(String groupId, String memberId, int generation) {
        send(new Request.Heartbeat(groupId, memberId, generation));
    }

    public void leaveGroup(String groupId, String memberId) {
        send(new Request.LeaveGroup(groupId, memberId));
    }

    /** @return partitions that were not saved because this member does not own them */
    public List<TopicPartition> commitOffsets(String groupId, String memberId, List<PartitionOffset> offsets) {
        return ((Response.CommitOffsets) send(new Request.CommitOffsets(groupId, memberId, offsets))).rejected();
    }

    /** One entry per partition, in the same order; offset -1 where nothing was committed. */
    public List<PartitionOffset> fetchOffsets(String groupId, List<TopicPartition> partitions) {
        return ((Response.FetchOffsets) send(new Request.FetchOffsets(groupId, partitions))).offsets();
    }

    // ---- core --------------------------------------------------------------------

    /**
     * Sends one request and waits for its response.
     *
     * @throws TributaryException with the broker's error code, or
     *                            {@link ErrorCode#NETWORK_ERROR} if the connection failed
     */
    public Response send(Request request) {
        lock.lock();
        try {
            for (int attempt = 1; ; attempt++) {
                try {
                    return sendOnce(request);
                } catch (TributaryException e) {
                    // Errors that another broker might not have: this one is not the leader,
                    // is unreachable, or is shutting down (a leader stopping mid-request).
                    boolean redirectable = e.error() == ErrorCode.NOT_LEADER
                            || e.error() == ErrorCode.NETWORK_ERROR
                            || e.error() == ErrorCode.BROKER_SHUTTING_DOWN;
                    if (!redirectable || attempt >= config.maxAttempts()) {
                        throw e;
                    }
                    disconnect();
                    String leader = leaderHint(e);
                    if (leader != null) {
                        if (!addresses.contains(leader)) {
                            addresses.add(leader);
                        }
                        current = addresses.indexOf(leader);
                        if (attempt == 1) {
                            continue; // a direct redirect: no need to wait
                        }
                    } else {
                        current = (current + 1) % addresses.size();
                    }
                    backoff(attempt);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Extracts "host:port" from a NOT_LEADER message such as "...; leader is host:port". */
    private static String leaderHint(TributaryException e) {
        if (e.error() != ErrorCode.NOT_LEADER) {
            return null;
        }
        String message = e.getMessage();
        int at = message.indexOf(LEADER_HINT);
        if (at < 0) {
            return null;
        }
        String hint = message.substring(at + LEADER_HINT.length()).trim();
        int colon = hint.lastIndexOf(':');
        return colon > 0 ? hint : null;
    }

    private void backoff(int attempt) {
        long ms = Math.min(MAX_BACKOFF_MS, 50L << Math.min(attempt, 10));
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TributaryException(ErrorCode.NETWORK_ERROR, "interrupted while retrying");
        }
    }

    private Response sendOnce(Request request) {
        // Called with the lock held.
        if (closed) {
            throw new IllegalStateException("client is closed");
        }
        int extraWait = switch (request) {
            case Request.Fetch f -> f.maxWaitMs();
            case Request.MultiFetch f -> f.maxWaitMs();
            default -> 0;
        };
        int correlationId = nextCorrelationId++;
        ResponseEnvelope response;
        try {
            connectIfNeeded();
            socket.setSoTimeout(config.requestTimeoutMs() + extraWait);
            Frames.write(out, Codec.encodeRequest(correlationId, config.clientId(), request));
            out.flush();
            byte[] frame = Frames.read(in, config.maxFrameBytes());
            if (frame == null) {
                throw new IOException("broker closed the connection");
            }
            response = Codec.decodeResponse(frame);
        } catch (IOException | ProtocolException e) {
            disconnect();
            throw new TributaryException(ErrorCode.NETWORK_ERROR, request.apiKey() + " to "
                    + addresses.get(current) + " failed: " + e.getMessage(), e);
        }
        if (response.correlationId() != correlationId) {
            disconnect();
            throw new TributaryException(ErrorCode.NETWORK_ERROR, "response for request "
                    + response.correlationId() + " arrived while waiting for " + correlationId);
        }
        if (response.error() != ErrorCode.NONE) {
            throw new TributaryException(response.error(), response.errorMessage());
        }
        return response.body();
    }

    private void connectIfNeeded() throws IOException {
        if (socket != null) {
            return;
        }
        Socket s = new Socket();
        try {
            s.setTcpNoDelay(true);
            String address = addresses.get(current);
            s.connect(new InetSocketAddress(ClientConfig.host(address), ClientConfig.port(address)),
                    config.connectTimeoutMs());
            in = new BufferedInputStream(s.getInputStream());
            out = new BufferedOutputStream(s.getOutputStream());
            socket = s;
        } catch (IOException e) {
            s.close();
            throw e;
        }
    }

    private void disconnect() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // nothing to do
            }
            socket = null;
            in = null;
            out = null;
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
            disconnect();
        } finally {
            lock.unlock();
        }
    }
}
