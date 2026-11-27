package io.github.karthika04.tributary.bench;

import java.util.Locale;

/**
 * One benchmark's outcome. Latencies are in milliseconds.
 *
 * @param targetRate records per second the run aimed for, or 0 for "as fast as possible"
 */
public record PerfResult(String scenario, String deployment, int recordSize, String compression, int producers,
                         int targetRate, long records, double recordsPerSec, double megabytesPerSec,
                         double p50, double p95, double p99, double p999, double max, long errors) {

    public static final String CSV_HEADER = "scenario,deployment,record_size,compression,producers,target_rate,"
            + "records,records_per_sec,mb_per_sec,p50_ms,p95_ms,p99_ms,p999_ms,max_ms,errors";

    public String csv() {
        return String.format(Locale.ROOT, "%s,%s,%d,%s,%d,%d,%d,%.0f,%.2f,%.3f,%.3f,%.3f,%.3f,%.3f,%d",
                scenario, deployment, recordSize, compression, producers, targetRate, records, recordsPerSec,
                megabytesPerSec, p50, p95, p99, p999, max, errors);
    }

    public String summary() {
        return String.format(Locale.ROOT,
                "%-11s %-8s %6d B %-5s %s: %,10.0f rec/s %8.2f MB/s | p50 %7.2f  p95 %7.2f  p99 %7.2f  p99.9 %7.2f  max %8.2f ms%s",
                scenario, deployment, recordSize, compression, targetRate == 0 ? "max rate    " : String.format(
                        Locale.ROOT, "%,6d rec/s", targetRate), recordsPerSec, megabytesPerSec, p50, p95, p99, p999,
                max, errors == 0 ? "" : "  (" + errors + " errors)");
    }
}
