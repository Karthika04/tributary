package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.Frames;
import java.util.ArrayList;
import java.util.List;

/**
 * Connection settings shared by every client.
 *
 * @param brokers          addresses ({@code host:port}) of one or more brokers. In a
 *                         cluster any of them will do: the client follows redirects to
 *                         the leader
 * @param requestTimeoutMs how long to wait for a broker's answer before giving up
 *                         (a fetch's own wait time is added on top)
 * @param maxAttempts      how many times one request is tried when the broker is
 *                         unreachable or is not the leader (for example during a
 *                         leader election), with a growing pause between tries
 */
public record ClientConfig(List<String> brokers, String clientId, int connectTimeoutMs, int requestTimeoutMs,
                           int maxFrameBytes, int maxAttempts) {

    public ClientConfig {
        brokers = List.copyOf(brokers);
        if (brokers.isEmpty()) {
            throw new IllegalArgumentException("at least one broker address is required");
        }
        for (String broker : brokers) {
            checkAddress(broker);
        }
        if (connectTimeoutMs <= 0 || requestTimeoutMs <= 0 || maxFrameBytes <= 0 || maxAttempts <= 0) {
            throw new IllegalArgumentException("timeouts and limits must be positive");
        }
    }

    public static ClientConfig of(String host, int port) {
        return of(List.of(host + ":" + port));
    }

    public static ClientConfig of(List<String> brokers) {
        return new ClientConfig(brokers, "tributary-client", 10_000, 30_000, Frames.DEFAULT_MAX_FRAME_BYTES, 5);
    }

    /** Parses {@code host:port}, or several separated by commas: {@code h1:9292,h2:9292}. */
    public static ClientConfig parse(String brokers) {
        List<String> list = new ArrayList<>();
        for (String part : brokers.split(",")) {
            if (!part.isBlank()) {
                list.add(part.trim());
            }
        }
        return of(list);
    }

    static String host(String address) {
        return address.substring(0, address.lastIndexOf(':'));
    }

    static int port(String address) {
        return Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
    }

    private static void checkAddress(String address) {
        int colon = address == null ? -1 : address.lastIndexOf(':');
        if (colon <= 0 || colon == address.length() - 1) {
            throw new IllegalArgumentException("expected host:port, got '" + address + "'");
        }
        int port;
        try {
            port = Integer.parseInt(address.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid port in '" + address + "'");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be 1-65535 in '" + address + "'");
        }
    }

    public ClientConfig withClientId(String value) {
        return new ClientConfig(brokers, value, connectTimeoutMs, requestTimeoutMs, maxFrameBytes, maxAttempts);
    }

    public ClientConfig withRequestTimeoutMs(int value) {
        return new ClientConfig(brokers, clientId, connectTimeoutMs, value, maxFrameBytes, maxAttempts);
    }

    public ClientConfig withConnectTimeoutMs(int value) {
        return new ClientConfig(brokers, clientId, value, requestTimeoutMs, maxFrameBytes, maxAttempts);
    }

    public ClientConfig withMaxAttempts(int value) {
        return new ClientConfig(brokers, clientId, connectTimeoutMs, requestTimeoutMs, maxFrameBytes, value);
    }
}
