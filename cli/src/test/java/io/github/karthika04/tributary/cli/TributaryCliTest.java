package io.github.karthika04.tributary.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.broker.BrokerConfig;
import io.github.karthika04.tributary.broker.BrokerServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TributaryCliTest {

    @TempDir
    Path dataDir;

    /** Runs one CLI command and captures what it prints. */
    private record Result(int exitCode, String out, String err) {
    }

    private static Result run(BrokerServer broker, String stdin, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String bootstrap = broker == null ? "localhost:1" : "localhost:" + broker.port();
        TributaryCli cli = new TributaryCli(new ByteArrayInputStream(stdin.getBytes(UTF_8)),
                new PrintStream(out, true, UTF_8), new PrintStream(err, true, UTF_8), bootstrap);
        int code = cli.run(args);
        return new Result(code, out.toString(UTF_8), err.toString(UTF_8));
    }

    private BrokerServer startBroker() throws IOException {
        return BrokerServer.start(BrokerConfig.defaults(dataDir).withPort(0));
    }

    @Test
    void createListProduceAndConsume() throws IOException {
        try (BrokerServer broker = startBroker()) {
            Result created = run(broker, "", "create-topic", "orders", "--partitions", "2");
            assertEquals(0, created.exitCode(), created.err());
            assertTrue(created.out().contains("Created topic orders with 2 partition(s)"));

            Result topics = run(broker, "", "topics");
            assertTrue(topics.out().contains("orders\t2 partition(s)"), topics.out());

            Result produced = run(broker, "", "produce", "orders", "--key", "user-42", "first", "second");
            assertEquals(0, produced.exitCode(), produced.err());
            assertTrue(produced.out().contains("at offset 0") && produced.out().contains("at offset 1"),
                    produced.out());

            Result consumed = run(broker, "", "consume", "orders", "--from-beginning", "--max-messages", "2");
            assertEquals(0, consumed.exitCode(), consumed.err());
            assertEquals("first\nsecond\n", consumed.out().replace("\r", ""));
        }
    }

    @Test
    void producesOneMessagePerStdinLine() throws IOException {
        try (BrokerServer broker = startBroker()) {
            run(broker, "", "create-topic", "lines");

            Result produced = run(broker, "alpha\nbeta\n\ngamma\n", "produce", "lines");
            assertEquals(0, produced.exitCode(), produced.err());

            Result consumed = run(broker, "", "consume", "lines", "--from-beginning", "--timeout-ms", "500",
                    "--print-metadata");
            assertEquals("lines-0@0\t-\talpha\nlines-0@1\t-\tbeta\nlines-0@2\t-\tgamma\n",
                    consumed.out().replace("\r", ""));
        }
    }

    @Test
    void showsOffsets() throws IOException {
        try (BrokerServer broker = startBroker()) {
            run(broker, "", "create-topic", "t", "--partitions", "2");
            run(broker, "", "produce", "t", "--partition", "1", "a", "b", "c");

            Result offsets = run(broker, "", "offsets", "t");

            assertTrue(offsets.out().contains("0\t0\t0\t0"), offsets.out());
            assertTrue(offsets.out().contains("1\t0\t3\t3"), offsets.out());
        }
    }

    @Test
    void reportsErrorsWithExitCodes() throws IOException {
        try (BrokerServer broker = startBroker()) {
            Result missing = run(broker, "", "offsets", "nope");
            assertEquals(1, missing.exitCode());
            assertTrue(missing.err().contains("UNKNOWN_TOPIC"), missing.err());

            run(broker, "", "create-topic", "dup");
            Result duplicate = run(broker, "", "create-topic", "dup");
            assertEquals(1, duplicate.exitCode());
            assertTrue(duplicate.err().contains("TOPIC_ALREADY_EXISTS"), duplicate.err());
        }
        assertEquals(2, run(null, "", "fly-to-the-moon").exitCode());
        assertEquals(2, run(null, "", "create-topic").exitCode());
        assertEquals(2, run(null, "", "create-topic", "x", "--partitions").exitCode());
        assertEquals(2, run(null, "").exitCode());
        assertEquals(0, run(null, "", "--help").exitCode());
    }

    @Test
    void reportsAnUnreachableBroker() {
        Result result = run(null, "", "topics");

        assertEquals(1, result.exitCode());
        assertTrue(result.err().contains("NETWORK_ERROR"), result.err());
    }
}
