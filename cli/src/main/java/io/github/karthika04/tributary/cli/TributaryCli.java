package io.github.karthika04.tributary.cli;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.Consumer;
import io.github.karthika04.tributary.client.ConsumerConfig;
import io.github.karthika04.tributary.client.ConsumerRecord;
import io.github.karthika04.tributary.client.Producer;
import io.github.karthika04.tributary.client.ProducerConfig;
import io.github.karthika04.tributary.client.RecordMetadata;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.client.TributaryException;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.TopicPartition;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * The {@code tributary} command-line tool.
 *
 * <pre>
 * tributary create-topic orders --partitions 3
 * tributary topics
 * tributary produce orders --key user-42 "clicked checkout"
 * echo "one line per message" | tributary produce orders
 * tributary consume orders --from-beginning
 * tributary offsets orders
 * </pre>
 *
 * Every command accepts {@code --bootstrap host:port} (default {@code localhost:9292},
 * or the {@code TRIBUTARY_BOOTSTRAP} environment variable).
 */
public final class TributaryCli {

    static final int OK = 0;
    static final int FAILED = 1;
    static final int USAGE = 2;

    private final InputStream stdin;
    private final PrintStream out;
    private final PrintStream err;
    private final String defaultBootstrap;

    TributaryCli(InputStream stdin, PrintStream out, PrintStream err, String defaultBootstrap) {
        this.stdin = stdin;
        this.out = out;
        this.err = err;
        this.defaultBootstrap = defaultBootstrap;
    }

    public static void main(String[] args) {
        String bootstrap = System.getenv().getOrDefault("TRIBUTARY_BOOTSTRAP", "localhost:9292");
        System.exit(new TributaryCli(System.in, System.out, System.err, bootstrap).run(args));
    }

    int run(String[] argv) {
        Args args;
        try {
            args = Args.parse(argv);
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            err.println(USAGE_TEXT);
            return USAGE;
        }
        if (args.command == null || args.has("help")) {
            out.println(USAGE_TEXT);
            return args.command == null && !args.has("help") ? USAGE : OK;
        }
        try {
            ClientConfig client = ClientConfig.parse(args.option("bootstrap", defaultBootstrap))
                    .withClientId("tributary-cli");
            return switch (args.command) {
                case "create-topic" -> createTopic(client, args);
                case "topics" -> listTopics(client);
                case "produce" -> produce(client, args);
                case "consume" -> consume(client, args);
                case "offsets" -> offsets(client, args);
                default -> {
                    err.println("error: unknown command '" + args.command + "'");
                    err.println(USAGE_TEXT);
                    yield USAGE;
                }
            };
        } catch (TributaryException e) {
            err.println("error: " + e.getMessage());
            return FAILED;
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            return USAGE;
        }
    }

    private int createTopic(ClientConfig config, Args args) {
        String topic = args.positional(0, "topic name");
        int partitions = Integer.parseInt(args.option("partitions", "1"));
        try (TributaryClient client = new TributaryClient(config)) {
            client.createTopic(topic, partitions);
        }
        out.println("Created topic " + topic + " with " + partitions + " partition(s)");
        return OK;
    }

    private int listTopics(ClientConfig config) {
        try (TributaryClient client = new TributaryClient(config)) {
            Map<String, Integer> topics = client.listTopics();
            if (topics.isEmpty()) {
                out.println("(no topics)");
            }
            topics.forEach((name, partitions) -> out.println(name + "\t" + partitions + " partition(s)"));
        }
        return OK;
    }

    private int produce(ClientConfig config, Args args) {
        String topic = args.positional(0, "topic name");
        String key = args.option("key", null);
        int partition = Integer.parseInt(args.option("partition", "-1"));
        List<String> messages = args.positionals.subList(1, args.positionals.size());

        List<CompletableFuture<RecordMetadata>> results = new ArrayList<>();
        try (Producer producer = new Producer(ProducerConfig.of(config))) {
            if (!messages.isEmpty()) {
                for (String message : messages) {
                    results.add(producer.send(topic, partition, bytes(key), bytes(message)));
                }
            } else {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(stdin, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.isEmpty()) {
                            results.add(producer.send(topic, partition, bytes(key), bytes(line)));
                        }
                    }
                } catch (IOException e) {
                    err.println("error reading input: " + e.getMessage());
                    return FAILED;
                }
            }
            producer.flush();
        }
        int failed = 0;
        for (CompletableFuture<RecordMetadata> result : results) {
            try {
                RecordMetadata meta = result.join();
                out.println("Sent to " + meta.topic() + "-" + meta.partition() + " at offset " + meta.offset());
            } catch (CompletionException e) {
                failed++;
                err.println("error: " + e.getCause().getMessage());
            }
        }
        return failed == 0 ? OK : FAILED;
    }

    private int consume(ClientConfig config, Args args) {
        String topic = args.positional(0, "topic name");
        long maxMessages = Long.parseLong(args.option("max-messages", Long.toString(Long.MAX_VALUE)));
        long idleTimeoutMs = Long.parseLong(args.option("timeout-ms", "-1"));
        boolean printMetadata = args.has("print-metadata");
        ConsumerConfig consumerConfig = ConsumerConfig.of(config).withAutoOffsetReset(args.has("from-beginning")
                ? ConsumerConfig.AutoOffsetReset.EARLIEST
                : ConsumerConfig.AutoOffsetReset.LATEST);

        try (Consumer consumer = new Consumer(consumerConfig)) {
            String partitionOption = args.option("partition", null);
            if (partitionOption == null) {
                consumer.subscribe(topic);
            } else {
                consumer.assign(List.of(new TopicPartition(topic, Integer.parseInt(partitionOption))));
            }
            consumer.assignment().forEach(consumer::position); // fix starting positions now

            long received = 0;
            long idleSince = System.currentTimeMillis();
            while (received < maxMessages) {
                List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    if (idleTimeoutMs >= 0 && System.currentTimeMillis() - idleSince >= idleTimeoutMs) {
                        break;
                    }
                    continue;
                }
                idleSince = System.currentTimeMillis();
                for (ConsumerRecord r : records) {
                    if (received >= maxMessages) {
                        break;
                    }
                    if (printMetadata) {
                        out.println(r.topic() + "-" + r.partition() + "@" + r.offset() + "\t"
                                + (r.key() == null ? "-" : r.keyAsString()) + "\t" + r.valueAsString());
                    } else {
                        out.println(r.valueAsString());
                    }
                    received++;
                }
            }
            out.flush();
        }
        return OK;
    }

    private int offsets(ClientConfig config, Args args) {
        String topic = args.positional(0, "topic name");
        try (TributaryClient client = new TributaryClient(config)) {
            int count = client.partitionCount(topic);
            out.println("PARTITION\tSTART\tEND\tRECORDS");
            for (int p = 0; p < count; p++) {
                Response.ListOffsets offsets = client.listOffsets(topic, p);
                out.println(p + "\t" + offsets.startOffset() + "\t" + offsets.endOffset() + "\t"
                        + (offsets.endOffset() - offsets.startOffset()));
            }
        }
        return OK;
    }

    private static byte[] bytes(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    static final String USAGE_TEXT = """
            usage: tributary <command> [arguments] [--bootstrap host:port]

            commands:
              create-topic <topic> [--partitions N]       create a topic (default 1 partition)
              topics                                      list topics
              produce <topic> [message ...]               send messages (or one per line from stdin)
                  [--key K] [--partition P]
              consume <topic>                             print messages as they arrive (Ctrl+C to stop)
                  [--from-beginning] [--partition P]
                  [--max-messages N] [--timeout-ms T]     stop after N messages or T ms with nothing new
                  [--print-metadata]                      show partition, offset and key too
              offsets <topic>                             show each partition's offset range

            --bootstrap defaults to localhost:9292 or the TRIBUTARY_BOOTSTRAP environment variable.""";

    /** Minimal argument parser: a command, positional arguments, and --options. */
    static final class Args {
        private static final List<String> FLAGS = List.of("from-beginning", "print-metadata", "help");

        String command;
        final List<String> positionals = new ArrayList<>();
        final Map<String, String> options = new HashMap<>();

        static Args parse(String[] argv) {
            Args args = new Args();
            for (int i = 0; i < argv.length; i++) {
                String arg = argv[i];
                if (arg.equals("-h")) {
                    arg = "--help";
                }
                if (arg.startsWith("--")) {
                    String name = arg.substring(2);
                    if (FLAGS.contains(name)) {
                        args.options.put(name, "true");
                    } else {
                        if (i + 1 >= argv.length) {
                            throw new IllegalArgumentException(arg + " needs a value");
                        }
                        args.options.put(name, argv[++i]);
                    }
                } else if (args.command == null) {
                    args.command = arg;
                } else {
                    args.positionals.add(arg);
                }
            }
            return args;
        }

        boolean has(String name) {
            return options.containsKey(name);
        }

        String option(String name, String defaultValue) {
            return options.getOrDefault(name, defaultValue);
        }

        String positional(int index, String what) {
            if (index >= positionals.size()) {
                throw new IllegalArgumentException("missing " + what);
            }
            return positionals.get(index);
        }
    }
}
