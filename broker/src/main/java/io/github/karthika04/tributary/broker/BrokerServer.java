package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.ApiKey;
import io.github.karthika04.tributary.protocol.Codec;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.Frames;
import io.github.karthika04.tributary.protocol.ProtocolException;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.RequestDecodeException;
import io.github.karthika04.tributary.protocol.RequestEnvelope;
import io.github.karthika04.tributary.protocol.ResponseEnvelope;
import io.github.karthika04.tributary.raft.RaftConfig;
import io.github.karthika04.tributary.raft.RaftNode;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.slf4j.MDC;

/**
 * The broker's network layer: accepts TCP connections and serves each one on its own
 * <em>virtual thread</em>.
 *
 * <p>Why virtual threads? The simplest server code is "one thread per connection,
 * read a request, handle it, write the answer, repeat". With ordinary threads that
 * stops scaling at a few thousand connections, because each thread costs about 1 MB
 * of memory. Virtual threads (Java 21) cost a few KB and park cheaply while waiting on
 * the network or a long-poll, so this straightforward style handles many thousands of
 * connections.
 *
 * <p>Requests on one connection are answered in order, one at a time.
 */
public final class BrokerServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(BrokerServer.class.getName());

    private final BrokerConfig config;
    private final TopicManager topics;
    private final GroupCoordinator groups;
    private final RaftNode raft; // null for a single broker
    private final BrokerRaftTransport raftTransport;
    private final RequestHandler handler;
    private final PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final BrokerMetrics metrics = new BrokerMetrics(meterRegistry);
    private final OffsetStore offsets;
    private final ServerSocket serverSocket;
    private final ExecutorService connectionThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService housekeeping;
    private final Set<Socket> openSockets = ConcurrentHashMap.newKeySet();
    private final Thread acceptThread;
    private volatile boolean running = true;

    private BrokerServer(BrokerConfig config, Clock clock) throws IOException {
        this.config = config;
        if (config.isClustered()) {
            // A cluster member's topics are rebuilt from the replicated Raft log on every
            // start, so they always match the other brokers exactly.
            deleteRecursively(config.dataDir().resolve("state"));
            this.topics = new TopicManager(config.dataDir().resolve("state"), config.logConfig());
        } else {
            this.topics = new TopicManager(config.dataDir(), config.logConfig());
        }
        RaftNode startedRaft = null;
        BrokerRaftTransport startedTransport = null;
        try {
            OffsetStore offsets = new OffsetStore(topics.internalTopic(OffsetStore.TOPIC), clock);
            LocalWrites local = new LocalWrites(topics, offsets);
            Writes writes = local;
            if (config.isClustered()) {
                Map<String, String> peers = new HashMap<>();
                for (String id : config.clusterNodes().keySet()) {
                    if (!id.equals(config.nodeId())) {
                        peers.put(id, config.peerAddress(id));
                    }
                }
                startedTransport = new BrokerRaftTransport(config.nodeId(), peers);
                RaftConfig raftConfig = RaftConfig.of(config.nodeId(), List.copyOf(peers.keySet()),
                        config.dataDir().resolve("raft"));
                startedRaft = RaftNode.start(raftConfig, startedTransport, new ClusterStateMachine(local));
                writes = new RaftWrites(startedRaft, config.clusterNodes());
            }
            this.raft = startedRaft;
            this.raftTransport = startedTransport;
            this.offsets = offsets;
            this.groups = new GroupCoordinator(topics, offsets, writes, clock);
            this.handler = new RequestHandler(topics, groups, writes, raft, config, clock, metrics);
            this.serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(config.port()));
        } catch (IOException | RuntimeException e) {
            if (startedRaft != null) {
                startedRaft.close();
            }
            if (startedTransport != null) {
                startedTransport.close();
            }
            metrics.close();
            topics.close();
            throw e;
        }
        this.housekeeping = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tributary-housekeeping");
            t.setDaemon(true);
            return t;
        });
        if (!config.isClustered()) {
            // In a cluster, deleting data must itself be replicated (Raft log compaction),
            // which Tributary does not do yet; see DESIGN.md.
            housekeeping.scheduleWithFixedDelay(this::runRetention, config.retentionCheckIntervalMs(),
                    config.retentionCheckIntervalMs(), TimeUnit.MILLISECONDS);
        }
        housekeeping.scheduleWithFixedDelay(this::expireGroupMembers, config.groupCheckIntervalMs(),
                config.groupCheckIntervalMs(), TimeUnit.MILLISECONDS);
        metrics.connections(openSockets);
        if (raft != null) {
            metrics.raft(raft);
        }
        housekeeping.scheduleWithFixedDelay(this::refreshMetrics, 0, 1, TimeUnit.SECONDS);
        this.acceptThread = Thread.ofPlatform().name("tributary-accept").start(this::acceptLoop);
    }

    /** Opens the data directory, binds the port and starts serving. */
    public static BrokerServer start(BrokerConfig config) throws IOException {
        return new BrokerServer(config, Clock.systemUTC());
    }

    static BrokerServer start(BrokerConfig config, Clock clock) throws IOException {
        return new BrokerServer(config, clock);
    }

    /** The port actually bound (differs from the config when it asked for port 0). */
    public int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true); // send small responses immediately
                openSockets.add(socket);
                connectionThreads.submit(() -> serve(socket));
            } catch (IOException e) {
                if (running) {
                    LOG.log(Level.WARNING, "accept failed", e);
                }
            }
        }
    }

    private void serve(Socket socket) {
        try (socket) {
            InputStream in = new BufferedInputStream(socket.getInputStream());
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            while (running) {
                byte[] frame = Frames.read(in, config.maxFrameBytes());
                if (frame == null) {
                    return; // client closed the connection
                }
                ResponseEnvelope response;
                Compression compression = Compression.NONE;
                long started = System.nanoTime();
                try {
                    RequestEnvelope request = Codec.decodeRequest(frame);
                    if (request.request() instanceof Request.Fetch fetch) {
                        compression = fetch.compression();
                    } else if (request.request() instanceof Request.MultiFetch fetch) {
                        compression = fetch.compression();
                    }
                    // Every log line written while handling this request carries these
                    // (the JSON logs show them as fields), so one request's story can be
                    // pulled out of a busy log.
                    MDC.put("clientId", String.valueOf(request.clientId()));
                    MDC.put("correlationId", Integer.toString(request.correlationId()));
                    MDC.put("api", request.request().apiKey().name());
                    try {
                        response = handler.handle(request);
                        logIfSlowOrFailed(response, System.nanoTime() - started);
                    } finally {
                        MDC.remove("clientId");
                        MDC.remove("correlationId");
                        MDC.remove("api");
                    }
                } catch (RequestDecodeException e) {
                    response = ResponseEnvelope.failure(e.correlationId(), e.apiKey(), e.error(), e.getMessage());
                }
                metrics.request(response.apiKey(), response.error(), System.nanoTime() - started);
                Frames.write(out, Codec.encodeResponse(response, compression));
                out.flush();
            }
        } catch (ProtocolException e) {
            LOG.log(Level.WARNING, "closing connection from " + socket.getRemoteSocketAddress() + ": "
                    + e.getMessage());
        } catch (SocketException e) {
            // connection reset or closed during shutdown; nothing to report
        } catch (IOException e) {
            LOG.log(Level.DEBUG, "connection error", e);
        } finally {
            openSockets.remove(socket);
        }
    }

    /** Requests slower than this are logged, so they can be found later. */
    static final long SLOW_REQUEST_NANOS = TimeUnit.SECONDS.toNanos(1);

    private static void logIfSlowOrFailed(ResponseEnvelope response, long nanos) {
        if (response.error() == ErrorCode.INTERNAL_ERROR) {
            LOG.log(Level.ERROR, "request failed: " + response.errorMessage());
        } else if (nanos > SLOW_REQUEST_NANOS && !isLongPoll(response.apiKey())) {
            LOG.log(Level.WARNING, "slow request: " + TimeUnit.NANOSECONDS.toMillis(nanos) + " ms");
        }
    }

    /** Fetches wait on purpose (long polling), so a slow one is not a problem. */
    private static boolean isLongPoll(ApiKey api) {
        return api == ApiKey.FETCH || api == ApiKey.MULTI_FETCH;
    }

    private void refreshMetrics() {
        try {
            metrics.refreshPartitions(topics);
            metrics.refreshLag(topics, offsets);
            if (raft != null) {
                metrics.sampleLeader(raft);
            }
        } catch (RuntimeException e) {
            LOG.log(Level.DEBUG, "metrics refresh failed", e);
        }
    }

    /** The broker's metrics in Prometheus' text format, as served on {@code /metrics}. */
    public String scrapeMetrics() {
        return meterRegistry.scrape();
    }

    private void expireGroupMembers() {
        try {
            groups.expireMembers();
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "consumer group check failed", e);
        }
    }

    GroupCoordinator groups() {
        return groups;
    }

    private void runRetention() {
        try {
            int deleted = topics.applyRetention();
            if (deleted > 0) {
                LOG.log(Level.INFO, "retention deleted " + deleted + " segment(s)");
            }
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "retention check failed", e);
        }
    }

    /**
     * Graceful shutdown, in an order that never loses an acknowledged write:
     * <ol>
     *   <li>stop accepting new connections;</li>
     *   <li>close client sockets, so idle connections end;</li>
     *   <li>wake long-polling fetches so they return now;</li>
     *   <li>let requests already being handled finish (threads are never interrupted,
     *       because interrupting a thread in the middle of a file write closes the file);</li>
     *   <li>close every log, which writes its clean-shutdown marker.</li>
     * </ol>
     */
    @Override
    public void close() {
        if (!running) {
            return;
        }
        running = false;
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // already closed
        }
        for (Socket socket : openSockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // already closed
            }
        }
        if (raft != null) {
            raft.close(); // fails writes still waiting for replication, so their requests end now
        }
        topics.stopWaiting();
        connectionThreads.shutdown();
        try {
            if (!connectionThreads.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.log(Level.WARNING, "some requests were still running after 10 seconds");
            }
            acceptThread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        housekeeping.shutdownNow();
        if (raftTransport != null) {
            raftTransport.close();
        }
        metrics.close();
        topics.close();
    }

    /** For tests: this broker's local copy of the topics. */
    TopicManager topics() {
        return topics;
    }

    /** For tests and tooling: this broker's Raft node, or null for a single broker. */
    RaftNode raft() {
        return raft;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
