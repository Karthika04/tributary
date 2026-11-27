package io.github.karthika04.tributary.broker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.protocol.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The /metrics endpoint, read over HTTP the way Prometheus scrapes it. */
class MetricsTest {

    @TempDir
    Path dir;

    /**
     * The value of a metric in Prometheus' text format: the first line named
     * {@code name} that carries every {@code key="value"} label given.
     */
    static double metric(String text, String name, String... labels) {
        for (String line : text.split("\n")) {
            if (line.startsWith("#") || !(line.startsWith(name + "{") || line.startsWith(name + " "))) {
                continue;
            }
            boolean all = true;
            for (String label : labels) {
                all &= line.contains(label);
            }
            if (all) {
                return Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
            }
        }
        throw new AssertionError("no " + name + " " + List.of(labels) + " in:\n" + text);
    }

    private static String scrape(int port) throws IOException {
        HttpURLConnection http = (HttpURLConnection) URI.create("http://localhost:" + port + "/metrics").toURL()
                .openConnection();
        try {
            assertEquals(200, http.getResponseCode());
            assertTrue(http.getContentType().startsWith("text/plain"), http.getContentType());
            try (InputStream in = http.getInputStream()) {
                return new String(in.readAllBytes(), UTF_8);
            }
        } finally {
            http.disconnect();
        }
    }

    private static List<ProducedRecord> records(int n) {
        List<ProducedRecord> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(new ProducedRecord(null, ("value-" + i).getBytes(UTF_8))); // 7 bytes each
        }
        return list;
    }

    @Test
    void countsRequestsRecordsAndBytesAndServesThemOverHttp() throws Exception {
        try (BrokerServer broker = BrokerServer.start(BrokerConfig.defaults(dir).withPort(0));
             MetricsEndpoint endpoint = MetricsEndpoint.start(0, broker::scrapeMetrics);
             TributaryClient client = new TributaryClient(ClientConfig.of("localhost", broker.port()))) {
            client.createTopic("orders", 2);
            client.produce("orders", 0, records(5), Compression.NONE);
            client.produce("orders", 1, records(2), Compression.NONE);
            Response.Fetch fetched = client.fetch("orders", 0, 0, 100, 0, Compression.NONE);
            assertEquals(5, fetched.records().size());
            try {
                client.produce("no-such-topic", 0, records(1), Compression.NONE);
            } catch (RuntimeException expected) {
                // an UNKNOWN_TOPIC error, which should be counted as one
            }

            String text = scrape(endpoint.port());

            assertEquals(7.0, metric(text, "tributary_produce_records_total", "topic=\"orders\""));
            assertEquals(49.0, metric(text, "tributary_produce_bytes_total", "topic=\"orders\""));
            assertEquals(5.0, metric(text, "tributary_fetch_records_total", "topic=\"orders\""));
            assertEquals(2.0, metric(text, "tributary_request_latency_seconds_count",
                    "api=\"produce\"", "error=\"none\""));
            assertEquals(1.0, metric(text, "tributary_request_latency_seconds_count",
                    "api=\"produce\"", "error=\"unknown_topic\""));
            assertTrue(metric(text, "tributary_connections") >= 1);
        }
    }

    @Test
    void reportsHowFarBehindEachConsumerGroupIs() throws Exception {
        try (BrokerServer broker = BrokerServer.start(BrokerConfig.defaults(dir).withPort(0));
             TributaryClient client = new TributaryClient(ClientConfig.of("localhost", broker.port()))) {
            client.createTopic("jobs", 1);
            client.produce("jobs", 0, records(5), Compression.NONE);
            Response.JoinGroup member = client.joinGroup("workers", "", List.of("jobs"), 10_000, "range");
            client.commitOffsets("workers", member.memberId(), List.of(new PartitionOffset("jobs", 0, 2)));

            // The broker adds gauges for new partitions and groups once a second.
            double lag = Double.NaN;
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                try {
                    lag = metric(broker.scrapeMetrics(), "tributary_consumer_lag", "group=\"workers\"",
                            "topic=\"jobs\"", "partition=\"0\"");
                    break;
                } catch (AssertionError notYet) {
                    Thread.sleep(100);
                }
            }
            assertEquals(3.0, lag, "5 records, committed up to offset 2");

            // Lag is computed when scraped, so new records show up at once.
            client.produce("jobs", 0, records(4), Compression.NONE);
            assertEquals(7.0, metric(broker.scrapeMetrics(), "tributary_consumer_lag", "group=\"workers\""));
            assertEquals(9.0, metric(broker.scrapeMetrics(), "tributary_partition_end_offset",
                    "topic=\"jobs\"", "partition=\"0\""));
        }
    }
}
