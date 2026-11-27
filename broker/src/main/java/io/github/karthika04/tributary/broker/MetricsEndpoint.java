package io.github.karthika04.tributary.broker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Serves {@code GET /metrics} in Prometheus' text format, for Prometheus to scrape.
 * Uses the HTTP server built into the JDK: one small endpoint does not justify a web
 * framework.
 */
public final class MetricsEndpoint implements AutoCloseable {

    /** The content type of Prometheus' text exposition format. */
    static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final HttpServer server;

    private MetricsEndpoint(HttpServer server) {
        this.server = server;
    }

    /**
     * @param port   the port to listen on, or 0 for any free port
     * @param scrape produces the current metrics text
     */
    public static MetricsEndpoint start(int port, Supplier<String> scrape) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> respond(exchange, scrape));
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        return new MetricsEndpoint(server);
    }

    private static void respond(HttpExchange exchange, Supplier<String> scrape) throws IOException {
        try (exchange) {
            if (!exchange.getRequestMethod().equals("GET")) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            byte[] body = scrape.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
