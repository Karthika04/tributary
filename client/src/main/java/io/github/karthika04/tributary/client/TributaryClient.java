package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.Codec;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.Frames;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.protocol.ProtocolException;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.ResponseEnvelope;
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
 * A low-level connection to one broker: send a request, get its response.
 *
 * <p>Connects lazily on first use and reconnects automatically after a network error.
 * Thread-safe: requests from several threads are sent one at a time. For real
 * applications, {@link Producer} and {@link Consumer} are easier to use.
 *
 * <pre>{@code
 * try (TributaryClient client = new TributaryClient(ClientConfig.of("localhost", 9292))) {
 *     client.createTopic("orders", 3);
 *     long offset = client.produce("orders", 0, List.of(new ProducedRecord(null, bytes)), Compression.NONE);
 * }
 * }</pre>
 */
public final class TributaryClient implements AutoCloseable {

    private final ClientConfig config;
    private final ReentrantLock lock = new ReentrantLock();
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private int nextCorrelationId;
    private boolean closed;

    public TributaryClient(ClientConfig config) {
        this.config = config;
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

    public Response.ListOffsets listOffsets(String topic, int partition) {
        return (Response.ListOffsets) send(new Request.ListOffsets(topic, partition));
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
            if (closed) {
                throw new IllegalStateException("client is closed");
            }
            int extraWait = request instanceof Request.Fetch f ? f.maxWaitMs() : 0;
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
                        + config.host() + ":" + config.port() + " failed: " + e.getMessage(), e);
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
        } finally {
            lock.unlock();
        }
    }

    private void connectIfNeeded() throws IOException {
        if (socket != null) {
            return;
        }
        Socket s = new Socket();
        try {
            s.setTcpNoDelay(true);
            s.connect(new InetSocketAddress(config.host(), config.port()), config.connectTimeoutMs());
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
