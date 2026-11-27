package io.github.karthika04.tributary.chaos;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * One broker running as its own JVM, so the chaos tests can fail it the way real
 * machines fail: {@code kill -9} (no shutdown hook, no flush), and
 * {@code SIGSTOP}/{@code SIGCONT} (frozen, like a long GC pause or a stalled VM,
 * then waking up still believing whatever it believed before).
 */
public final class BrokerProcess {

    private static final String MAIN = "io.github.karthika04.tributary.broker.BrokerMain";

    private final String nodeId;
    private final List<String> args;
    private final Path logFile;
    private Process process;
    private boolean paused;

    BrokerProcess(String nodeId, List<String> brokerArgs, Path logFile) {
        this.nodeId = nodeId;
        this.args = List.copyOf(brokerArgs);
        this.logFile = logFile;
    }

    public String nodeId() {
        return nodeId;
    }

    public synchronized boolean isRunning() {
        return process != null && process.isAlive();
    }

    public synchronized boolean isPaused() {
        return paused;
    }

    /** Starts the broker with this JVM's own class path, which holds the broker and its dependencies. */
    public synchronized void start() throws IOException {
        if (isRunning()) {
            throw new IllegalStateException("broker " + nodeId + " is already running");
        }
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", // small and quick to start
                "-cp", System.getProperty("java.class.path"),
                MAIN));
        command.addAll(args);
        process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()))
                .start();
        paused = false;
    }

    /** Kills the process at once: SIGKILL, so nothing gets a chance to clean up. */
    public synchronized void kill() throws InterruptedException {
        if (process == null) {
            return;
        }
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
        paused = false;
    }

    /** Asks for a clean shutdown (SIGTERM runs the broker's shutdown hook). */
    public synchronized void stop() throws IOException, InterruptedException {
        if (process == null || !process.isAlive()) {
            return;
        }
        if (paused) {
            resume();
        }
        process.destroy();
        if (!process.waitFor(15, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
        }
    }

    /** Freezes the process with SIGSTOP: its sockets stay open but nothing answers. */
    public synchronized void pause() throws IOException, InterruptedException {
        if (isRunning() && !paused) {
            signal("-STOP");
            paused = true;
        }
    }

    /** Wakes a frozen process with SIGCONT. */
    public synchronized void resume() throws IOException, InterruptedException {
        if (isRunning() && paused) {
            signal("-CONT");
            paused = false;
        }
    }

    private void signal(String signal) throws IOException, InterruptedException {
        Process kill = new ProcessBuilder("kill", signal, Long.toString(process.pid())).inheritIO().start();
        if (kill.waitFor() != 0) {
            throw new IOException("kill " + signal + " " + process.pid() + " failed");
        }
    }
}
