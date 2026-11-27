package io.github.karthika04.tributary.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.broker.BrokerConfig;
import io.github.karthika04.tributary.broker.BrokerServer;
import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.protocol.Compression;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Short runs of the load generator against an in-process broker. Not a measurement:
 * it checks the tool works, because a benchmark that quietly reports 0 records per
 * second (as the first run of this one did, which exposed a producer bug) is worse
 * than no benchmark.
 */
class PerfSmokeTest {

    @TempDir
    Path dir;

    @Test
    void maxRateAndFixedRateRunsProduceRecordsWithoutErrors() throws Exception {
        try (BrokerServer broker = BrokerServer.start(BrokerConfig.defaults(dir).withPort(0))) {
            List<String> brokers = List.of("localhost:" + broker.port());
            try (TributaryClient admin = new TributaryClient(ClientConfig.of(brokers))) {
                admin.createTopic("smoke", 3);
                admin.createTopic("smoke-e2e", 3);
            }
            ProducePerf.Settings d = ProducePerf.Settings.defaults("smoke");
            for (int rate : List.of(0, 2_000)) {
                ProducePerf.Settings s = new ProducePerf.Settings("smoke", 10_000, rate, 2, 1, 1, d.lingerMs(),
                        d.batchBytes(), Compression.NONE, false, d.maxOutstanding());

                PerfResult r = ProducePerf.run("smoke", "single", brokers, s);

                assertEquals(0, r.errors(), r.summary());
                assertTrue(r.records() > 0 && r.recordsPerSec() > 0, r.summary());
                assertTrue(r.p50() > 0 && r.p50() <= r.p99() && r.p99() <= r.max(), r.summary());
            }

            PerfResult e2e = EndToEndPerf.run("single", brokers, "smoke-e2e", 100, 500, 1, 1, 5);
            assertEquals(0, e2e.errors(), "records produced but never consumed: " + e2e.summary());
            assertTrue(e2e.records() >= 400, e2e.summary());
        }
    }

    @Test
    void theMarkdownTableHasARowPerResult() {
        PerfResult r = new PerfResult("latency", "cluster", 1_000, "none", 1, 1_000, 10_000, 1_000, 0.95, 4.5, 7.3,
                9.8, 13.7, 17.0, 0);
        String md = Suite.markdown(List.of(r, r));
        assertEquals(4, md.lines().count()); // header, separator, two rows
        assertTrue(md.contains("| latency | cluster | 1,000 B | none | 1,000/s | 1,000 | 1.0 | 4.50 | 9.80 | 13.70 |"),
                md);
    }
}
