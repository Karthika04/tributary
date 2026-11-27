package io.github.karthika04.tributary.bench;

import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * Records latencies in microseconds into an HdrHistogram.
 *
 * <p>Why HdrHistogram rather than a list of samples or a running average: it keeps
 * every value to 3 significant digits across a huge range (1 us to a minute here) in
 * fixed, small memory, so percentiles like p99.9 are exact enough to trust no matter
 * how many millions of samples are recorded. Averages hide exactly the slow requests
 * that people notice.
 */
final class LatencyRecorder {

    private static final long HIGHEST_MICROS = TimeUnit.MINUTES.toMicros(1);

    private final Histogram histogram = new Histogram(HIGHEST_MICROS, 3);

    /** Thread-safe: completions arrive on the client's threads. */
    synchronized void recordNanos(long nanos) {
        long micros = Math.max(0, nanos / 1_000);
        histogram.recordValue(Math.min(micros, HIGHEST_MICROS)); // clamp, rather than throw, past a minute
    }

    synchronized long count() {
        return histogram.getTotalCount();
    }

    /** The latency at {@code percentile} (0 to 100), in milliseconds. */
    synchronized double millisAt(double percentile) {
        return histogram.getTotalCount() == 0 ? 0 : histogram.getValueAtPercentile(percentile) / 1_000.0;
    }

    synchronized double maxMillis() {
        return histogram.getMaxValue() / 1_000.0;
    }
}
