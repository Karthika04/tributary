package io.github.karthika04.tributary.chaos;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.protocol.FetchedRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The checker decides whether a chaos run passed, so it gets tests of its own. */
class CheckerTest {

    private static Workload.Attempt acked(long seq, long offset) {
        return new Workload.Attempt(0, seq, offset, 0, 0);
    }

    private static Workload.Attempt failed(long seq) {
        return new Workload.Attempt(0, seq, -1, 0, 0);
    }

    /** Partition 0's contents: values of producer 0 in log order, at offsets 0, 1, 2... */
    private static List<FetchedRecord> stored(String... values) {
        List<FetchedRecord> records = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            records.add(new FetchedRecord(i, 0, null, values[i].getBytes(UTF_8)));
        }
        return records;
    }

    private static Checker.Result check(List<Workload.Attempt> attempts, List<FetchedRecord> stored) {
        return Checker.check(Map.of(0, attempts), Map.of(0, stored));
    }

    @Test
    void aCleanRunPasses() {
        Checker.Result r = check(List.of(acked(0, 0), acked(1, 1), acked(2, 2)), stored("p0-0", "p0-1", "p0-2"));
        assertTrue(r.ok());
        assertEquals(3, r.acknowledged());
    }

    @Test
    void detectsALostAcknowledgedWrite() {
        Checker.Result r = check(List.of(acked(0, 0), acked(1, 1)), stored("p0-0"));
        assertFalse(r.ok());
        assertEquals(1, r.lost());
    }

    @Test
    void detectsAnAcknowledgedOffsetHoldingSomethingElse() {
        Checker.Result r = check(List.of(acked(0, 1), acked(1, 0)), stored("p0-0", "p0-1"));
        assertFalse(r.ok());
        assertEquals(2, r.wrongOffsets());
    }

    @Test
    void detectsValuesOutOfOrder() {
        Checker.Result r = check(List.of(failed(0), failed(1)), stored("p0-1", "p0-0"));
        assertFalse(r.ok());
        assertEquals(1, r.outOfOrder());
    }

    @Test
    void detectsAValueFromTheWrongProducer() {
        Checker.Result r = check(List.of(acked(0, 0)), stored("p0-0", "p1-0"));
        assertFalse(r.ok());
        assertEquals(1, r.foreign());
    }

    @Test
    void duplicatesAndFailedButStoredWritesAreAllowed() {
        // seq 0 was retried (stored twice); seq 1 timed out but had committed anyway.
        Checker.Result r = check(List.of(acked(0, 1), failed(1), acked(2, 3)),
                stored("p0-0", "p0-0", "p0-1", "p0-2"));
        assertTrue(r.ok(), r.details().toString());
        assertEquals(1, r.duplicates());
        assertEquals(1, r.failedButStored());
    }

    @Test
    void aFailedWriteThatWasNeverStoredIsFine() {
        Checker.Result r = check(List.of(acked(0, 0), failed(1), acked(2, 1)), stored("p0-0", "p0-2"));
        assertTrue(r.ok());
        assertEquals(1, r.failed());
        assertEquals(0, r.failedButStored());
    }

    @Test
    void raftLogsMayDifferInLengthButNeverInContent() {
        byte[] a = {1};
        byte[] b = {2};
        assertNull(Checker.checkRaftLogs(Map.of("1", List.of(a, b), "2", List.of(a), "3", List.of(a, b))));
        String problem = Checker.checkRaftLogs(Map.of("1", List.of(a, b), "2", List.of(a, a)));
        assertTrue(problem != null && problem.contains("index 2"), problem);
    }
}
