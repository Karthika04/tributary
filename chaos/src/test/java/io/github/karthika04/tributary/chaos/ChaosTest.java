package io.github.karthika04.tributary.chaos;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every fault once, against three real broker processes. This is the short version
 * that runs on every build; {@link ChaosRunner} runs hundreds of rounds.
 */
class ChaosTest {

    @TempDir
    Path dir;

    @Test
    void everyFaultOnceLosesNothingAcknowledged() throws Exception {
        List<Fault> faults = List.of(Fault.values());
        ChaosRunner runner = new ChaosRunner(42, System.out, faults, 1_500, 2_500);

        List<ChaosRunner.RoundResult> results = runner.runCluster(0, dir, faults.size(), 0);

        assertTrue(results.size() == faults.size(), "expected one result per fault (and no Raft log violation): "
                + results);
        for (ChaosRunner.RoundResult r : results) {
            assertTrue(r.check().ok(), r.fault() + " (" + r.injected() + "): " + r.check().details());
            assertTrue(r.check().acknowledged() > 0, r.fault() + ": nothing was acknowledged at all");
        }
    }
}
