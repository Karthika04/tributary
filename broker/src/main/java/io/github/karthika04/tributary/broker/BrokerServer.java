package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.Codec;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.Frames;
import io.github.karthika04.tributary.protocol.ProtocolException;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.RequestDecodeException;
import io.github.karthika04.tributary.protocol.RequestEnvelope;
import io.github.karthika04.tributary.protocol.ResponseEnvelope;
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
import java.time.Clock;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final RequestHandler handler;
    private final ServerSocket serverSocket;
    private final ExecutorService connectionThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService housekeeping;
    private final Set<Socket> openSockets = ConcurrentHashMap.newKeySet();
    private final Thread acceptThread;
    private volatile boolean running = true;

    private BrokerServer(BrokerConfig config, Clock clock) throws IOException {
        this.config = config;
        this.topics = new TopicManager(config.dataDir(), config.logConfig());
        try {
            OffsetStore offsets = new OffsetStore(topics.internalTopic(OffsetStore.TOPIC), clock);
            this.groups = new GroupCoordinator(topics, offsets, clock);
        } catch (RuntimeException e) {
            topics.close();
            throw e;
        }
        this.handler = new RequestHandler(topics, groups, config, clock);
        try {
            this.serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(config.port()));
        } catch (IOException e) {
            topics.close();
            throw e;
        }
        this.housekeeping = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tributary-retention");
            t.setDaemon(true);
            return t;
        });
        housekeeping.scheduleWithFixedDelay(this::runRetention, config.retentionCheckIntervalMs(),
                config.retentionCheckIntervalMs(), TimeUnit.MILLISECONDS);
        housekeeping.scheduleWithFixedDelay(this::expireGroupMembers, config.groupCheckIntervalMs(),
                config.groupCheckIntervalMs(), TimeUnit.MILLISECONDS);
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
                try {
                    RequestEnvelope request = Codec.decodeRequest(frame);
                    if (request.request() instanceof Request.Fetch fetch) {
                        compression = fetch.compression();
                    }
                    response = handler.handle(request);
                } catch (RequestDecodeException e) {
                    response = ResponseEnvelope.failure(e.correlationId(), e.apiKey(), e.error(), e.getMessage());
                }
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
        topics.close();
    }
}
