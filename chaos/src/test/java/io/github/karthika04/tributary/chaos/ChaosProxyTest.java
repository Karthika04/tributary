package io.github.karthika04.tributary.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ChaosProxyTest {

    private ServerSocket echo;
    private ChaosProxy proxy;

    /** A server that echoes every byte back. */
    private void startEcho() throws IOException {
        echo = new ServerSocket(0);
        Thread.ofVirtual().start(() -> {
            while (true) {
                Socket s;
                try {
                    s = echo.accept();
                } catch (IOException e) {
                    return;
                }
                Thread.ofVirtual().start(() -> {
                    try (s; InputStream in = s.getInputStream(); OutputStream out = s.getOutputStream()) {
                        in.transferTo(out);
                    } catch (IOException ignored) {
                        // closed
                    }
                });
            }
        });
        proxy = new ChaosProxy(echo.getLocalPort());
    }

    @AfterEach
    void close() throws IOException {
        proxy.close();
        echo.close();
    }

    private Socket connect() throws IOException {
        Socket s = new Socket("localhost", proxy.port());
        s.setSoTimeout(500);
        return s;
    }

    private static int roundTrip(Socket s, int b) throws IOException {
        s.getOutputStream().write(b);
        s.getOutputStream().flush();
        return s.getInputStream().read();
    }

    @Test
    void forwardsBothWays() throws IOException {
        startEcho();
        try (Socket s = connect()) {
            assertEquals(42, roundTrip(s, 42));
        }
    }

    @Test
    void aCutLinkDropsConnectionsAndBlackHolesNewOnes() throws IOException {
        startEcho();
        Socket before = connect();
        assertEquals(1, roundTrip(before, 1));

        proxy.cut();
        // The open connection is dropped.
        assertThrows(IOException.class, () -> {
            int b = roundTrip(before, 2);
            if (b == -1) {
                throw new IOException("closed");
            }
        });
        // A new connection is accepted but nothing comes back: it hangs, like a partition.
        try (Socket during = connect()) {
            assertThrows(SocketTimeoutException.class, () -> roundTrip(during, 3));
        }

        proxy.heal();
        try (Socket after = connect()) {
            assertEquals(4, roundTrip(after, 4));
        }
        before.close();
    }

    @Test
    void latencyDelaysTraffic() throws IOException {
        startEcho();
        proxy.setLatencyMs(150);
        try (Socket s = connect()) {
            s.setSoTimeout(2_000);
            long start = System.nanoTime();
            assertEquals(7, roundTrip(s, 7));
            long ms = (System.nanoTime() - start) / 1_000_000;
            assertTrue(ms >= 280, "two delayed hops should take at least 300 ms, took " + ms);
        }
    }
}
