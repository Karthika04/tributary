package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.Frames;

/**
 * Connection settings shared by every client.
 *
 * @param requestTimeoutMs how long to wait for a broker's answer before giving up
 *                         (a fetch's own wait time is added on top)
 */
public record ClientConfig(String host, int port, String clientId, int connectTimeoutMs, int requestTimeoutMs,
                           int maxFrameBytes) {

    public ClientConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host is required");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be 1-65535, got " + port);
        }
        if (connectTimeoutMs <= 0 || requestTimeoutMs <= 0 || maxFrameBytes <= 0) {
            throw new IllegalArgumentException("timeouts and limits must be positive");
        }
    }

    public static ClientConfig of(String host, int port) {
        return new ClientConfig(host, port, "tributary-client", 10_000, 30_000, Frames.DEFAULT_MAX_FRAME_BYTES);
    }

    /** Parses {@code host:port}, e.g. {@code localhost:9292}. */
    public static ClientConfig parse(String hostAndPort) {
        int colon = hostAndPort.lastIndexOf(':');
        if (colon <= 0 || colon == hostAndPort.length() - 1) {
            throw new IllegalArgumentException("expected host:port, got '" + hostAndPort + "'");
        }
        return of(hostAndPort.substring(0, colon), Integer.parseInt(hostAndPort.substring(colon + 1)));
    }

    public ClientConfig withClientId(String value) {
        return new ClientConfig(host, port, value, connectTimeoutMs, requestTimeoutMs, maxFrameBytes);
    }

    public ClientConfig withRequestTimeoutMs(int value) {
        return new ClientConfig(host, port, clientId, connectTimeoutMs, value, maxFrameBytes);
    }
}
