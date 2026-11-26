package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.storage.LogConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * Starts a broker from the command line:
 *
 * <pre>
 * tributary-broker [--port 9292] [--data-dir ./data] [--retention-hours 168]
 *                  [--retention-bytes N] [--segment-bytes N] [--flush-every-append]
 * </pre>
 *
 * Ctrl+C shuts it down gracefully.
 */
public final class BrokerMain {

    private BrokerMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BrokerConfig config;
        try {
            config = parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(usage());
            System.exit(2);
            return;
        }
        if (config == null) {
            System.out.println(usage());
            return;
        }

        BrokerServer broker = BrokerServer.start(config);
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            broker.close();
            stopped.countDown();
        }, "tributary-shutdown"));
        System.out.println("Tributary broker listening on port " + broker.port()
                + ", data in " + config.dataDir().toAbsolutePath());
        stopped.await();
    }

    /** @return the config, or null if help was requested */
    static BrokerConfig parse(String[] args) {
        int port = BrokerConfig.DEFAULT_PORT;
        Path dataDir = Path.of("data");
        LogConfig log = LogConfig.defaults();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--help", "-h" -> {
                    return null;
                }
                case "--port" -> port = Integer.parseInt(value(args, ++i, arg));
                case "--data-dir" -> dataDir = Path.of(value(args, ++i, arg));
                case "--retention-hours" -> log = log.withRetentionMs(
                        Duration.ofHours(Long.parseLong(value(args, ++i, arg))).toMillis());
                case "--retention-bytes" -> log = log.withRetentionBytes(Long.parseLong(value(args, ++i, arg)));
                case "--segment-bytes" -> log = log.withSegmentBytes(Integer.parseInt(value(args, ++i, arg)));
                case "--flush-every-append" -> log = log.withFlushEveryAppend(true);
                default -> throw new IllegalArgumentException("unknown option " + arg);
            }
        }
        return BrokerConfig.defaults(dataDir).withPort(port).withLogConfig(log);
    }

    private static String value(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException(option + " needs a value");
        }
        return args[index];
    }

    static String usage() {
        return """
                usage: tributary-broker [options]
                  --port N               TCP port (default 9292)
                  --data-dir PATH        where topic data is stored (default ./data)
                  --retention-hours N    delete data older than N hours (default: keep forever)
                  --retention-bytes N    keep at most N bytes per partition (default: no limit)
                  --segment-bytes N      segment file size (default 64 MiB)
                  --flush-every-append   force every write to disk (slower, safest)
                  --help                 show this message""";
    }
}
