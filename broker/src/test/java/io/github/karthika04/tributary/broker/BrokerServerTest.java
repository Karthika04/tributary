package io.github.karthika04.tributary.broker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.client.TributaryException;
import io.github.karthika04.tributary.protocol.ApiKey;
import io.github.karthika04.tributary.protocol.Codec;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.FetchedRecord;
import io.github.karthika04.tributary.protocol.Frames;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.ResponseEnvelope;
import io.github.karthika04.tributary.storage.LogConfig;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerServerTest {

    @TempDir
    Path dataDir;

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private BrokerConfig config() {
        return BrokerConfig.defaults(dataDir).withPort(0);
    }

    private static TributaryClient connect(BrokerServer broker) {
        return new TributaryClient(ClientConfig.of("localhost", broker.port()));
    }

    private static List<ProducedRecord> records(String... values) {
        List<ProducedRecord> list = new ArrayList<>();
        for (String v : values) {
            list.add(new ProducedRecord(null, bytes(v)));
        }
        return list;
    }

    private static ErrorCode errorOf(Runnable action) {
        return assertThrows(TributaryException.class, action::run).error();
    }

    // ---- topics ------------------------------------------------------------------

    @Test
    void createsAndListsTopics() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config()); TributaryClient client = connect(broker)) {
            client.createTopic("payments", 1);
            client.createTopic("orders", 3);

            assertEquals(Map.of("orders", 3, "payments", 1), client.listTopics());
            assertEquals(List.of("orders", "payments"), List.copyOf(client.listTopics().keySet()));
        }
    }

    @Test
    void rejectsBadTopicRequests() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config()); TributaryClient client = connect(broker)) {
            client.createTopic("orders", 2);

            assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, errorOf(() -> client.createTopic("orders", 2)));
            assertEquals(ErrorCode.INVALID_TOPIC, errorOf(() -> client.createTopic("bad/name", 1)));
            assertEquals(ErrorCode.INVALID_TOPIC, errorOf(() -> client.createTopic("..", 1)));
            assertEquals(ErrorCode.INVALID_REQUEST, errorOf(() -> client.createTopic("zero", 0)));
            assertEquals(ErrorCode.UNKNOWN_TOPIC, errorOf(() -> client.listOffsets("missing", 0)));
            assertEquals(ErrorCode.UNKNOWN_PARTITION, errorOf(() -> client.listOffsets("orders", 2)));
        }
    }

    // ---- produce and fetch -------------------------------------------------------

    @Test
    void producedRecordsComeBackInOrderWithBrokerTimestamps() throws IOException {
        Clock fixed = Clock.fixed(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC);
        try (BrokerServer broker = BrokerServer.start(config(), fixed); TributaryClient client = connect(broker)) {
            client.createTopic("orders", 1);

            assertEquals(0, client.produce("orders", 0, records("a", "b"), Compression.NONE));
            assertEquals(2, client.produce("orders", 0,
                    List.of(new ProducedRecord(bytes("user-1"), bytes("c"))), Compression.NONE));

            Response.Fetch fetch = client.fetch("orders", 0, 0, 100, 0, Compression.NONE);
            assertEquals(0, fetch.startOffset());
            assertEquals(3, fetch.endOffset());
            assertEquals(List.of(
                    new FetchedRecord(0, 1_700_000_000_000L, null, bytes("a")),
                    new FetchedRecord(1, 1_700_000_000_000L, null, bytes("b")),
                    new FetchedRecord(2, 1_700_000_000_000L, bytes("user-1"), bytes("c"))), fetch.records());
        }
    }

    @Test
    void compressionWorksInBothDirections() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config()); TributaryClient client = connect(broker)) {
            client.createTopic("logs", 1);
            List<ProducedRecord> batch = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                batch.add(new ProducedRecord(null, bytes("{\"level\":\"INFO\",\"msg\":\"request " + i + " ok\"}")));
            }
            client.produce("logs", 0, batch, Compression.GZIP);

            Response.Fetch fetch = client.fetch("logs", 0, 0, 1000, 0, Compression.GZIP);

            assertEquals(500, fetch.records().size());
            assertEquals("{\"level\":\"INFO\",\"msg\":\"request 499 ok\"}",
                    new String(fetch.records().get(499).value(), UTF_8));
        }
    }

    @Test
    void fetchRespectsMaxRecordsAndReportsRange() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config()); TributaryClient client = connect(broker)) {
            client.createTopic("t", 1);
            client.produce("t", 0, records("0", "1", "2", "3", "4"), Compression.NONE);

            Response.Fetch fetch = client.fetch("t", 0, 1, 2, 0, Compression.NONE);

            assertEquals(2, fetch.records().size());
            assertEquals(1, fetch.records().get(0).offset());
            assertEquals(5, fetch.endOffset());
            assertEquals(0, client.fetch("t", 0, 5, 10, 0, Compression.NONE).records().size());
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE,
                    errorOf(() -> client.fetch("t", 0, 6, 10, 0, Compression.NONE)));
            assertEquals(ErrorCode.INVALID_REQUEST,
                    errorOf(() -> client.fetch("t", 0, -1, 10, 0, Compression.NONE)));
        }
    }

    @Test
    void rejectsARecordThatIsTooLarge() throws IOException {
        LogConfig small = LogConfig.defaults().withMaxRecordBytes(1024);
        try (BrokerServer broker = BrokerServer.start(config().withLogConfig(small));
             TributaryClient client = connect(broker)) {
            client.createTopic("t", 1);
            List<ProducedRecord> tooBig = List.of(new ProducedRecord(null, new byte[5000]));

            assertEquals(ErrorCode.RECORD_TOO_LARGE, errorOf(() -> client.produce("t", 0, tooBig, Compression.NONE)));
            assertEquals(0, client.listOffsets("t", 0).endOffset());
        }
    }

    // ---- long polling ------------------------------------------------------------

    @Test
    void aWaitingFetchReturnsAsSoonAsDataArrives() throws Exception {
        try (BrokerServer broker = BrokerServer.start(config());
             TributaryClient admin = connect(broker);
             TributaryClient waiter = connect(broker)) {
            admin.createTopic("t", 1);
            long start = System.nanoTime();
            CompletableFuture<Response.Fetch> fetch = CompletableFuture.supplyAsync(
                    () -> waiter.fetch("t", 0, 0, 10, 10_000, Compression.NONE));

            Thread.sleep(200);
            assertTrue(!fetch.isDone(), "fetch should still be waiting");
            admin.produce("t", 0, records("hello"), Compression.NONE);

            Response.Fetch result = fetch.get(5, TimeUnit.SECONDS);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertEquals(1, result.records().size());
            assertTrue(elapsedMs < 3_000, "took " + elapsedMs + " ms");
        }
    }

    @Test
    void aWaitingFetchGivesUpAfterItsWaitTime() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config()); TributaryClient client = connect(broker)) {
            client.createTopic("t", 1);
            long start = System.nanoTime();

            Response.Fetch fetch = client.fetch("t", 0, 0, 10, 300, Compression.NONE);

            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertEquals(0, fetch.records().size());
            assertTrue(elapsedMs >= 250, "returned after only " + elapsedMs + " ms");
        }
    }

    @Test
    void shutdownWakesWaitingFetchesInsteadOfWaitingForThem() throws Exception {
        BrokerServer broker = BrokerServer.start(config());
        try (TributaryClient admin = connect(broker); TributaryClient waiter = connect(broker)) {
            admin.createTopic("t", 1);
            CompletableFuture<Void> fetch = CompletableFuture.runAsync(
                    () -> waiter.fetch("t", 0, 0, 10, 30_000, Compression.NONE));
            Thread.sleep(200);

            long start = System.nanoTime();
            broker.close();
            long closeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertTrue(closeMs < 5_000, "shutdown took " + closeMs + " ms");
            Exception e = assertThrows(Exception.class, () -> fetch.get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof TributaryException, "unexpected: " + e);
        }
    }

    // ---- durability --------------------------------------------------------------

    @Test
    void dataSurvivesABrokerRestart() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config()); TributaryClient client = connect(broker)) {
            client.createTopic("orders", 2);
            client.produce("orders", 1, records("kept"), Compression.NONE);
        }
        try (BrokerServer broker = BrokerServer.start(config()); TributaryClient client = connect(broker)) {
            assertEquals(Map.of("orders", 2), client.listTopics());
            Response.Fetch fetch = client.fetch("orders", 1, 0, 10, 0, Compression.NONE);
            assertEquals("kept", new String(fetch.records().get(0).value(), UTF_8));
            assertEquals(1, client.produce("orders", 1, records("next"), Compression.NONE));
        }
    }

    @Test
    void retentionRunsInTheBackground() throws Exception {
        LogConfig small = LogConfig.defaults().withMaxRecordBytes(512).withSegmentBytes(1024).withRetentionBytes(2048);
        BrokerConfig config = config().withLogConfig(small).withRetentionCheckIntervalMs(50);
        try (BrokerServer broker = BrokerServer.start(config); TributaryClient client = connect(broker)) {
            client.createTopic("t", 1);
            for (int i = 0; i < 200; i++) {
                client.produce("t", 0, records("message number " + i), Compression.NONE);
            }
            long deadline = System.currentTimeMillis() + 5_000;
            while (client.listOffsets("t", 0).startOffset() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(client.listOffsets("t", 0).startOffset() > 0, "retention never ran");
            assertEquals(200, client.listOffsets("t", 0).endOffset());
        }
    }

    // ---- concurrency -------------------------------------------------------------

    @Test
    void manyClientsAtOnce() throws Exception {
        try (BrokerServer broker = BrokerServer.start(config()); TributaryClient admin = connect(broker)) {
            admin.createTopic("busy", 4);
            int clients = 50;
            int perClient = 100;
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> tasks = new ArrayList<>();
                for (int c = 0; c < clients; c++) {
                    int id = c;
                    tasks.add(pool.submit(() -> {
                        try (TributaryClient client = connect(broker)) {
                            for (int i = 0; i < perClient; i++) {
                                client.produce("busy", (id + i) % 4, records("c" + id + "-" + i), Compression.NONE);
                            }
                        }
                        return null;
                    }));
                }
                for (Future<?> task : tasks) {
                    task.get(30, TimeUnit.SECONDS);
                }
            }
            long total = 0;
            for (int p = 0; p < 4; p++) {
                total += admin.listOffsets("busy", p).endOffset();
            }
            assertEquals(clients * perClient, total);
        }
    }

    // ---- bad input on the wire ---------------------------------------------------

    @Test
    void aMalformedRequestGetsAnErrorAndTheConnectionStillWorks() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config());
             Socket socket = new Socket("localhost", broker.port())) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // A LIST_OFFSETS header with a body that stops halfway.
            byte[] valid = Codec.encodeRequest(77, "raw", new Request.ListOffsets("t", 0));
            Frames.write(out, java.util.Arrays.copyOf(valid, valid.length - 2));
            out.flush();
            ResponseEnvelope error = Codec.decodeResponse(Frames.read(in, 1 << 20));
            assertEquals(77, error.correlationId());
            assertEquals(ErrorCode.INVALID_REQUEST, error.error());

            Frames.write(out, Codec.encodeRequest(78, "raw", new Request.Metadata()));
            out.flush();
            ResponseEnvelope ok = Codec.decodeResponse(Frames.read(in, 1 << 20));
            assertEquals(78, ok.correlationId());
            assertEquals(ApiKey.METADATA, ok.apiKey());
            assertEquals(ErrorCode.NONE, ok.error());
        }
    }

    @Test
    void garbageClosesTheConnection() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config());
             Socket socket = new Socket("localhost", broker.port())) {
            socket.setSoTimeout(5_000);
            Frames.write(socket.getOutputStream(), new byte[] {0x7f, 0x7f, 1, 2, 3});
            socket.getOutputStream().flush();

            assertNull(Frames.read(socket.getInputStream(), 1 << 20));
        }
    }

    @Test
    void anOversizedFrameClosesTheConnection() throws IOException {
        try (BrokerServer broker = BrokerServer.start(config());
             Socket socket = new Socket("localhost", broker.port())) {
            socket.setSoTimeout(5_000);
            new DataOutputStream(socket.getOutputStream()).writeInt(Integer.MAX_VALUE);
            socket.getOutputStream().flush();

            assertNull(Frames.read(socket.getInputStream(), 1 << 20));
        }
    }

    // ---- command line ------------------------------------------------------------

    @Test
    void parsesCommandLineOptions() {
        BrokerConfig config = BrokerMain.parse(new String[] {
                "--port", "9999", "--data-dir", "/tmp/x", "--retention-hours", "2", "--flush-every-append"});

        assertEquals(9999, config.port());
        assertEquals(Path.of("/tmp/x"), config.dataDir());
        assertEquals(7_200_000, config.logConfig().retentionMs());
        assertTrue(config.logConfig().flushEveryAppend());
        assertNull(BrokerMain.parse(new String[] {"--help"}));
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.parse(new String[] {"--nope"}));
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.parse(new String[] {"--port"}));
    }
}
