package io.github.karthika04.tributary.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogTest {

    /** Tiny segments so tests cross segment boundaries after a handful of records. */
    private static final LogConfig SMALL = LogConfig.defaults()
            .withMaxRecordBytes(512) // shrink this first: segmentBytes may not go below it
            .withSegmentBytes(1024)
            .withIndexBytes(64 * Index.ENTRY_SIZE);

    @TempDir
    Path dir;

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static String text(Record r) {
        return new String(r.value(), UTF_8);
    }

    private static List<Path> files(Path dir, String glob) throws IOException {
        List<Path> result = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            stream.forEach(result::add);
        }
        result.sort(null);
        return result;
    }

    /** Simulates a crash: the last close never finished, so its marker is missing. */
    private void forgetCleanShutdown() throws IOException {
        Files.delete(dir.resolve(Log.CLEAN_SHUTDOWN_FILE));
    }

    private Path newestLogFile() throws IOException {
        List<Path> logs = files(dir, "*" + Segment.LOG_SUFFIX);
        return logs.get(logs.size() - 1);
    }

    // ---- basics ------------------------------------------------------------------

    @Test
    void anEmptyLogStartsAtOffsetZero() {
        try (Log log = Log.open(dir, SMALL)) {
            assertEquals(0, log.startOffset());
            assertEquals(0, log.endOffset());
            assertEquals(List.of(), log.read(0, 10));
        }
    }

    @Test
    void appendsGetConsecutiveOffsets() {
        try (Log log = Log.open(dir, SMALL)) {
            assertEquals(0, log.append(null, bytes("a")));
            assertEquals(1, log.append(null, bytes("b")));
            assertEquals(2, log.appendAll(List.of(
                    NewRecord.of(1, bytes("c")), NewRecord.of(1, bytes("d")))));
            assertEquals(4, log.endOffset());
        }
    }

    @Test
    void readsBackExactlyWhatWasWritten() {
        try (Log log = Log.open(dir, SMALL)) {
            log.append(new NewRecord(111, bytes("user-1"), bytes("signed up")));
            log.append(new NewRecord(222, null, new byte[0]));

            Record first = log.read(0);
            assertEquals(0, first.offset());
            assertEquals(111, first.timestamp());
            assertArrayEquals(bytes("user-1"), first.key());
            assertEquals("signed up", text(first));

            Record second = log.read(1);
            assertNull(second.key());
            assertEquals(0, second.value().length);
        }
    }

    @Test
    void readingOutsideTheLogThrows() {
        try (Log log = Log.open(dir, SMALL)) {
            log.append(null, bytes("only"));

            OffsetOutOfRangeException e = assertThrows(OffsetOutOfRangeException.class, () -> log.read(1));
            assertEquals(0, e.startOffset());
            assertEquals(1, e.endOffset());
            assertThrows(OffsetOutOfRangeException.class, () -> log.read(-1));
            assertThrows(OffsetOutOfRangeException.class, () -> log.read(5, 10));
        }
    }

    @Test
    void readingFromTheEndMeansCaughtUp() {
        try (Log log = Log.open(dir, SMALL)) {
            log.append(null, bytes("x"));
            assertEquals(List.of(), log.read(1, 10));
        }
    }

    // ---- segments ----------------------------------------------------------------

    @Test
    void rollsToNewSegmentsAndReadsAcrossThem() throws IOException {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 200; i++) {
                log.append(null, bytes("message-" + i));
            }
            assertTrue(log.segmentCount() > 5, "expected several segments, got " + log.segmentCount());

            List<Record> all = log.read(0, 1000);
            assertEquals(200, all.size());
            for (int i = 0; i < 200; i++) {
                assertEquals(i, all.get(i).offset());
                assertEquals("message-" + i, text(all.get(i)));
            }
            List<Record> middle = log.read(57, 30);
            assertEquals(30, middle.size());
            assertEquals(57, middle.get(0).offset());
            assertEquals(86, middle.get(29).offset());
        }
        // Segment files are named after their first offset and every one is non-empty.
        List<Path> logs = files(dir, "*" + Segment.LOG_SUFFIX);
        assertEquals("00000000000000000000.log", logs.get(0).getFileName().toString());
        assertEquals(logs.size(), files(dir, "*" + Segment.INDEX_SUFFIX).size());
    }

    @Test
    void rollsWhenTheIndexIsFullEvenIfBytesRemain() {
        LogConfig fewPerSegment = SMALL.withIndexBytes(4 * Index.ENTRY_SIZE);
        try (Log log = Log.open(dir, fewPerSegment)) {
            for (int i = 0; i < 10; i++) {
                log.append(null, bytes("" + i));
            }
            assertEquals(3, log.segmentCount()); // 4 + 4 + 2
            assertEquals("9", text(log.read(9)));
        }
    }

    @Test
    void aBatchBiggerThanASegmentContinuesInTheNextOne() {
        try (Log log = Log.open(dir, SMALL)) {
            log.append(null, bytes("first"));
            List<NewRecord> big = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                big.add(NewRecord.of(1, bytes("batched-" + i))); // about 4 KB, segments hold 1 KB
            }

            assertEquals(1, log.appendAll(big));

            assertEquals(101, log.endOffset());
            assertTrue(log.segmentCount() >= 4, "segments: " + log.segmentCount());
            List<Record> all = log.read(0, 200);
            for (int i = 0; i < 100; i++) {
                assertEquals("batched-" + i, text(all.get(i + 1)));
            }
        }
    }

    @Test
    void handlesOneHundredThousandRecords() {
        LogConfig config = LogConfig.defaults().withMaxRecordBytes(1024).withSegmentBytes(256 * 1024);
        try (Log log = Log.open(dir, config)) {
            List<NewRecord> batch = new ArrayList<>();
            for (int i = 0; i < 100_000; i++) {
                batch.add(new NewRecord(i, bytes("k" + (i % 100)), bytes("v" + i)));
                if (batch.size() == 500) {
                    log.appendAll(batch);
                    batch.clear();
                }
            }
            assertEquals(100_000, log.endOffset());
            long offset = 0;
            while (offset < log.endOffset()) {
                for (Record r : log.read(offset, 5_000)) {
                    assertEquals(offset, r.offset());
                    assertEquals("v" + offset, text(r));
                    offset++;
                }
            }
        }
    }

    // ---- truncation --------------------------------------------------------------

    @Test
    void truncateFromRemovesTheTailAndAppendsReuseTheOffsets() {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 100; i++) {
                log.append(null, bytes("m" + i));
            }
            int segmentsBefore = log.segmentCount();

            log.truncateFrom(37);

            assertEquals(37, log.endOffset());
            assertTrue(log.segmentCount() < segmentsBefore);
            assertEquals("m36", text(log.read(36)));
            assertThrows(OffsetOutOfRangeException.class, () -> log.read(37));
            assertEquals(37, log.append(null, bytes("replacement")));
            assertEquals("replacement", text(log.read(37)));
            for (int i = 0; i < 100; i++) {
                log.append(null, bytes("more" + i)); // keeps rolling normally afterwards
            }
            assertEquals("more99", text(log.read(137)));
        }
    }

    @Test
    void truncationSurvivesARestartAndACrash() throws IOException {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 60; i++) {
                log.append(null, bytes("m" + i));
            }
            log.truncateFrom(10);
            log.append(null, bytes("new10"));
        }
        try (Log log = Log.open(dir, SMALL)) {
            assertEquals(11, log.endOffset());
            assertEquals("new10", text(log.read(10)));
            log.truncateFrom(5);
        }
        forgetCleanShutdown();
        try (Log log = Log.open(dir, SMALL)) {
            assertEquals(5, log.endOffset());
            assertEquals("m4", text(log.read(4)));
        }
    }

    @Test
    void truncateEdgeCases() {
        try (Log log = Log.open(dir, SMALL)) {
            log.truncateFrom(0); // empty log: nothing to do
            for (int i = 0; i < 50; i++) {
                log.append(null, bytes("m" + i));
            }
            log.truncateFrom(50); // at the end: nothing to do
            assertEquals(50, log.endOffset());

            log.truncateFrom(0); // everything

            assertEquals(0, log.endOffset());
            assertEquals(1, log.segmentCount());
            assertEquals(0, log.append(null, bytes("fresh")));
        }
    }

    // ---- validation --------------------------------------------------------------

    @Test
    void rejectsAnOversizedRecordAndWritesNothingFromItsBatch() {
        try (Log log = Log.open(dir, SMALL)) {
            log.append(null, bytes("before"));
            List<NewRecord> batch = List.of(
                    NewRecord.of(1, bytes("fine")),
                    NewRecord.of(1, new byte[600]));

            assertThrows(IllegalArgumentException.class, () -> log.appendAll(batch));
            assertEquals(1, log.endOffset());
            assertThrows(IllegalArgumentException.class, () -> log.appendAll(List.of()));
        }
    }

    @Test
    void aDirectoryCanOnlyBeOpenedOnceAtATime() {
        Log first = Log.open(dir, SMALL);
        try {
            assertThrows(IllegalStateException.class, () -> Log.open(dir, SMALL));
        } finally {
            first.close();
        }
        // ...and it can be opened again once closed.
        Log.open(dir, SMALL).close();
    }

    @Test
    void aClosedLogRejectsUse() {
        Log log = Log.open(dir, SMALL);
        log.close();
        log.close(); // closing twice is harmless
        assertThrows(IllegalStateException.class, () -> log.append(null, bytes("late")));
    }

    // ---- restarts and crash recovery --------------------------------------------

    @Test
    void keepsEverythingAcrossACleanRestart() {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 50; i++) {
                log.append(null, bytes("m" + i));
            }
        }
        try (Log log = Log.open(dir, SMALL)) {
            assertEquals(0, log.recoveredBytesDropped());
            assertEquals(50, log.endOffset());
            assertEquals("m49", text(log.read(49)));
            assertEquals(50, log.append(null, bytes("after restart")));
            assertEquals("after restart", text(log.read(50)));
        }
    }

    @Test
    void dropsAHalfWrittenRecordAfterACrash() throws IOException {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 20; i++) {
                log.append(null, bytes("m" + i));
            }
        }
        // The process died while writing record 20: only part of its frame reached disk.
        forgetCleanShutdown();
        byte[] full = RecordCodec.encode(new Record(20, 0, null, bytes("never finished"))).array();
        int written = full.length / 2;
        try (RandomAccessFile file = new RandomAccessFile(newestLogFile().toFile(), "rw")) {
            file.seek(file.length());
            file.write(full, 0, written);
        }

        try (Log log = Log.open(dir, SMALL)) {
            assertEquals(written, log.recoveredBytesDropped());
            assertEquals(20, log.endOffset());
            assertEquals("m19", text(log.read(19)));
            assertEquals(20, log.append(null, bytes("m20 again")));
            assertEquals("m20 again", text(log.read(20)));
        }

        // Crash a second time. If the first recovery had left the broken bytes in the
        // file, this recovery would stop at them and lose "m20 again".
        forgetCleanShutdown();
        try (Log log = Log.open(dir, SMALL)) {
            assertEquals(0, log.recoveredBytesDropped());
            assertEquals(21, log.endOffset());
            assertEquals("m20 again", text(log.read(20)));
        }
    }

    @Test
    void dropsADamagedLastRecordAfterACrash() throws IOException {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 20; i++) {
                log.append(null, bytes("m" + i));
            }
        }
        forgetCleanShutdown();
        try (RandomAccessFile file = new RandomAccessFile(newestLogFile().toFile(), "rw")) {
            long lastByte = file.length() - 1;
            file.seek(lastByte);
            int b = file.read();
            file.seek(lastByte);
            file.write(b ^ 0xFF);
        }

        try (Log log = Log.open(dir, SMALL)) {
            assertTrue(log.recoveredBytesDropped() > 0);
            assertEquals(19, log.endOffset());
            assertEquals("m18", text(log.read(18)));
        }
    }

    @Test
    void rebuildsTheActiveIndexLeftFullSizeByACrash() throws IOException {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 5; i++) {
                log.append(null, bytes("m" + i));
            }
        }
        // While a segment is active its index file is pre-sized and padded with zeros.
        // A crash leaves it that way instead of shrinking it on close.
        forgetCleanShutdown();
        List<Path> indexes = files(dir, "*" + Segment.INDEX_SUFFIX);
        try (RandomAccessFile file = new RandomAccessFile(indexes.get(indexes.size() - 1).toFile(), "rw")) {
            file.setLength(SMALL.indexBytes());
        }

        try (Log log = Log.open(dir, SMALL)) {
            assertEquals(5, log.endOffset());
            assertEquals("m4", text(log.read(4)));
        }
    }

    @Test
    void rebuildsAMissingIndexOfAnOldSegment() throws IOException {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 100; i++) {
                log.append(null, bytes("m" + i));
            }
        }
        Files.delete(files(dir, "*" + Segment.INDEX_SUFFIX).get(0));

        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 100; i++) {
                assertEquals("m" + i, text(log.read(i)));
            }
        }
    }

    @Test
    void neverReturnsDamagedBytesFromAnOldSegment() throws IOException {
        try (Log log = Log.open(dir, SMALL)) {
            for (int i = 0; i < 100; i++) {
                log.append(null, bytes("m" + i));
            }
        }
        Path oldest = files(dir, "*" + Segment.LOG_SUFFIX).get(0);
        try (RandomAccessFile file = new RandomAccessFile(oldest.toFile(), "rw")) {
            file.seek(RecordCodec.FRAME_OVERHEAD); // first byte of record 0's value
            file.write('X');
        }

        try (Log log = Log.open(dir, SMALL)) {
            assertThrows(CorruptRecordException.class, () -> log.read(0));
            assertEquals("m1", text(log.read(1)));
        }
    }

    // ---- retention ---------------------------------------------------------------

    @Test
    void timeRetentionDeletesOnlyExpiredSegments() {
        MutableClock clock = new MutableClock(1_000_000);
        LogConfig config = SMALL.withRetentionMs(Duration.ofHours(1).toMillis());
        try (Log log = Log.open(dir, config, clock)) {
            for (int i = 0; i < 100; i++) {
                log.append(null, bytes("old-" + i));
            }
            int segmentsBefore = log.segmentCount();
            clock.advance(Duration.ofHours(2));
            log.append(null, bytes("new")); // offset 100, lands in the active segment

            int deleted = log.applyRetention();

            assertTrue(deleted > 0);
            assertTrue(log.segmentCount() < segmentsBefore);
            assertTrue(log.startOffset() > 0);
            assertThrows(OffsetOutOfRangeException.class, () -> log.read(0));
            assertEquals("new", text(log.read(100)));
            // Nothing else is old enough now.
            assertEquals(0, log.applyRetention());
        }
    }

    @Test
    void timeRetentionNeverDeletesTheActiveSegment() {
        MutableClock clock = new MutableClock(0);
        try (Log log = Log.open(dir, SMALL.withRetentionMs(1), clock)) {
            log.append(null, bytes("only"));
            clock.advance(Duration.ofDays(30));

            assertEquals(0, log.applyRetention());
            assertEquals("only", text(log.read(0)));
        }
    }

    @Test
    void sizeRetentionKeepsTheLogUnderItsLimit() {
        try (Log log = Log.open(dir, SMALL.withRetentionBytes(3000))) {
            for (int i = 0; i < 300; i++) {
                log.append(null, bytes("message-" + i));
            }
            log.applyRetention();

            assertTrue(log.sizeInBytes() <= 3000, "size " + log.sizeInBytes());
            assertEquals(300, log.endOffset());
            assertEquals("message-299", text(log.read(299)));
        }
    }

    @Test
    void retentionSurvivesARestart() {
        try (Log log = Log.open(dir, SMALL.withRetentionBytes(2000))) {
            for (int i = 0; i < 200; i++) {
                log.append(null, bytes("message-" + i));
            }
            log.applyRetention();
        }
        try (Log log = Log.open(dir, SMALL)) {
            assertTrue(log.startOffset() > 0);
            assertEquals(200, log.endOffset());
            assertEquals("message-" + log.startOffset(), text(log.read(log.startOffset())));
        }
    }

    // ---- concurrency -------------------------------------------------------------

    @Test
    void manyWritersAndReadersAtOnce() throws Exception {
        int writers = 8;
        int perWriter = 2_000;
        try (Log log = Log.open(dir, SMALL.withSegmentBytes(64 * 1024).withIndexBytes(1024 * Index.ENTRY_SIZE))) {
            ExecutorService pool = Executors.newFixedThreadPool(writers + 4);
            CountDownLatch start = new CountDownLatch(1);
            AtomicBoolean writing = new AtomicBoolean(true);
            List<Future<?>> tasks = new ArrayList<>();

            for (int w = 0; w < writers; w++) {
                int id = w;
                tasks.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        log.append(null, bytes("w" + id + "-" + i));
                    }
                    return null;
                }));
            }
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                readers.add(pool.submit(() -> {
                    start.await();
                    while (writing.get()) {
                        long end = log.endOffset();
                        if (end > 0) {
                            long offset = ThreadLocalRandom.current().nextLong(end);
                            assertEquals(offset, log.read(offset).offset());
                        }
                    }
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> task : tasks) {
                task.get();
            }
            writing.set(false);
            for (Future<?> reader : readers) {
                reader.get();
            }
            pool.shutdown();

            int total = writers * perWriter;
            assertEquals(total, log.endOffset());
            Set<String> seen = new HashSet<>();
            long offset = 0;
            while (offset < total) {
                for (Record r : log.read(offset, 1000)) {
                    assertEquals(offset++, r.offset());
                    assertTrue(seen.add(text(r)), "duplicate " + text(r));
                }
            }
            assertEquals(total, seen.size());
        }
    }
}
