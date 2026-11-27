package io.github.karthika04.tributary.chaos;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TCP proxy for one direction of one link between brokers, which the chaos tests
 * can break on purpose.
 *
 * <ul>
 *   <li>{@link #cut()} behaves like a network partition: existing connections are
 *       dropped, and new ones are accepted but go nowhere, so requests hang until the
 *       sender's timeout instead of failing at once. That is what a dead network
 *       looks like to a program, and the harder case to handle.</li>
 *   <li>{@link #setLatencyMs} delays every chunk of data, like a slow network.</li>
 * </ul>
 */
public final class ChaosProxy implements AutoCloseable {

    private final ServerSocket server;
    private final int targetPort;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private volatile boolean cut;
    private volatile int latencyMs;
    private volatile boolean closed;

    /** Listens on a free local port and forwards to {@code localhost:targetPort}. */
    public ChaosProxy(int targetPort) throws IOException {
        this.targetPort = targetPort;
        this.server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress("localhost", 0));
        Thread.ofVirtual().name("chaos-proxy-" + server.getLocalPort()).start(this::acceptLoop);
    }

    public int port() {
        return server.getLocalPort();
    }

    public boolean isCut() {
        return cut;
    }

    /** Partitions this link: drops what is open, and black-holes what comes next. */
    public void cut() {
        cut = true;
        closeAll();
    }

    /** Restores the link. Black-holed connections are closed so their owners reconnect. */
    public void heal() {
        cut = false;
        closeAll();
    }

    public void setLatencyMs(int ms) {
        latencyMs = ms;
    }

    private void acceptLoop() {
        while (!closed) {
            Socket client;
            try {
                client = server.accept();
            } catch (IOException e) {
                return; // closed
            }
            sockets.add(client);
            if (cut) {
                continue; // a black hole: hold it open, forward nothing
            }
            Thread.ofVirtual().start(() -> connect(client));
        }
    }

    private void connect(Socket client) {
        Socket upstream = new Socket();
        try {
            upstream.connect(new InetSocketAddress("localhost", targetPort), 1_000);
        } catch (IOException e) {
            closeQuietly(client); // the broker is down: refuse, as the real network would
            closeQuietly(upstream);
            return;
        }
        sockets.add(upstream);
        if (cut) { // cut while connecting
            closeQuietly(client);
            closeQuietly(upstream);
            return;
        }
        Thread.ofVirtual().start(() -> pump(client, upstream));
        Thread.ofVirtual().start(() -> pump(upstream, client));
    }

    private void pump(Socket from, Socket to) {
        byte[] buffer = new byte[16 * 1024];
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            int n;
            while ((n = in.read(buffer)) != -1) {
                if (cut) {
                    break;
                }
                int delay = latencyMs;
                if (delay > 0) {
                    Thread.sleep(delay);
                }
                out.write(buffer, 0, n);
                out.flush();
            }
        } catch (IOException | InterruptedException e) {
            // one side closed; close both below
        } finally {
            closeQuietly(from);
            closeQuietly(to);
        }
    }

    private void closeAll() {
        for (Socket s : sockets) {
            closeQuietly(s);
        }
    }

    private void closeQuietly(Socket s) {
        sockets.remove(s);
        try {
            s.close();
        } catch (IOException ignored) {
            // already closed
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            server.close();
        } catch (IOException ignored) {
            // already closed
        }
        closeAll();
    }
}
