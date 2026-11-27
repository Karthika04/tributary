package io.github.karthika04.tributary.raft;

import static io.github.karthika04.tributary.raft.Cluster.await;
import static io.github.karthika04.tributary.raft.Cluster.propose;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftNodeTest {

    @TempDir
    Path dir;

    private static List<String> commands(String prefix, int count) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(prefix + i);
        }
        return list;
    }

    /** Proposes each command and waits for it to be committed and applied. */
    private static void proposeAll(RaftNode leader, List<String> commands) throws Exception {
        List<CompletableFuture<Object>> futures = new ArrayList<>();
        for (String c : commands) {
            futures.add(propose(leader, c));
        }
        for (CompletableFuture<Object> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
    }

    private static void awaitApplied(Cluster cluster, String id, List<String> expected) {
        await(() -> cluster.machines.get(id).applied().equals(expected),
                id + " to apply " + expected.size() + " commands");
    }

    // ---- elections -----------------------------------------------------------------

    @Test
    void aClusterElectsExactlyOneLeader() {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();

            Cluster.sleep(500); // several heartbeat rounds later, still exactly one
            int leaders = 0;
            for (RaftNode node : cluster.nodes.values()) {
                if (node.isLeader()) {
                    leaders++;
                } else {
                    await(() -> leader.nodeId().equals(node.leaderId()), node.nodeId() + " to learn the leader");
                    assertEquals(leader.currentTerm(), node.currentTerm());
                }
            }
            assertEquals(1, leaders);
        }
    }

    @Test
    void aSingleNodeClusterLeadsItself() throws Exception {
        try (Cluster cluster = new Cluster(dir, 1)) {
            RaftNode leader = cluster.awaitLeader();
            assertEquals("ok:x", propose(leader, "x").get(5, TimeUnit.SECONDS));
        }
    }

    /** The rule that keeps committed data safe (5.4.1): never vote for a candidate whose log is behind. */
    @Test
    void refusesToVoteForACandidateWhoseLogIsBehind() throws Exception {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();
            proposeAll(leader, commands("data-", 5));
            RaftNode follower = cluster.nodes.values().stream().filter(n -> n != leader).findFirst().orElseThrow();
            await(() -> follower.lastLogIndex() == leader.lastLogIndex(), "the follower to catch up");
            long term = follower.currentTerm();
            long lastIndex = follower.lastLogIndex();

            VoteResponse empty = follower.handleVote(new VoteRequest(term + 5, "stale", 0, 0, false));
            VoteResponse shorter = follower.handleVote(new VoteRequest(term + 6, "shorter", lastIndex - 1, term, false));
            VoteResponse equal = follower.handleVote(new VoteRequest(term + 7, "equal", lastIndex, leader.currentTerm(), false));

            assertFalse(empty.granted(), "voted for a candidate with an empty log");
            assertFalse(shorter.granted(), "voted for a candidate missing the last entry");
            assertTrue(equal.granted(), "should vote for a candidate whose log is as up to date");
            assertEquals(term + 7, equal.term());
        }
    }

    @Test
    void aFollowerThatMissedCommittedEntriesNeverBecomesLeader() throws Exception {
        for (int attempt = 0; attempt < 3; attempt++) {
            Path attemptDir = dir.resolve("attempt-" + attempt);
            try (Cluster cluster = new Cluster(attemptDir, 3)) {
                RaftNode leader = cluster.awaitLeader();
                List<String> others = new ArrayList<>(cluster.ids);
                others.remove(leader.nodeId());
                String stale = others.get(0);
                String fresh = others.get(1);

                cluster.network.isolate(stale, cluster.ids);
                List<String> committed = commands("committed-", 10);
                proposeAll(leader, committed); // committed by the leader and "fresh" alone
                cluster.kill(leader.nodeId());
                cluster.network.healAll();

                RaftNode next = cluster.awaitLeaderAmong(others);
                assertEquals(fresh, next.nodeId(), "the node missing committed entries was elected");
                awaitApplied(cluster, stale, committed);
            }
        }
    }

    // ---- replication ---------------------------------------------------------------

    @Test
    void committedCommandsReachEveryNodeInOrder() throws Exception {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();
            List<String> commands = commands("cmd-", 200);

            proposeAll(leader, commands);

            for (String id : cluster.ids) {
                awaitApplied(cluster, id, commands);
            }
        }
    }

    @Test
    void proposeReturnsTheStateMachinesResultOrRejection() throws Exception {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();

            assertEquals("ok:hello", propose(leader, "hello").get(5, TimeUnit.SECONDS));
            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> propose(leader, "reject-me").get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            // A rejected command fails the same way everywhere and does not stop later ones.
            assertEquals("ok:after", propose(leader, "after").get(5, TimeUnit.SECONDS));
            for (String id : cluster.ids) {
                awaitApplied(cluster, id, List.of("hello", "after"));
            }
        }
    }

    @Test
    void followersRefuseProposalsAndNameTheLeader() {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();
            RaftNode follower = cluster.nodes.values().stream().filter(n -> n != leader).findFirst().orElseThrow();
            await(() -> leader.nodeId().equals(follower.leaderId()), "the follower to learn the leader");

            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> propose(follower, "nope").get(5, TimeUnit.SECONDS));

            assertTrue(e.getCause() instanceof NotLeaderException);
            assertEquals(leader.nodeId(), ((NotLeaderException) e.getCause()).leaderId());
        }
    }

    // ---- failures ------------------------------------------------------------------

    @Test
    void aNewLeaderTakesOverAndKeepsEveryCommittedCommand() throws Exception {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode first = cluster.awaitLeader();
            List<String> before = commands("before-", 50);
            proposeAll(first, before);

            cluster.kill(first.nodeId());
            List<String> survivors = new ArrayList<>(cluster.ids);
            survivors.remove(first.nodeId());
            RaftNode second = cluster.awaitLeaderAmong(survivors);
            assertNotEquals(first.nodeId(), second.nodeId());
            List<String> after = commands("after-", 50);
            proposeAll(second, after);

            List<String> all = new ArrayList<>(before);
            all.addAll(after);
            for (String id : survivors) {
                awaitApplied(cluster, id, all);
            }
            // The old leader comes back with its files, rejoins as a follower and catches up.
            cluster.start(first.nodeId());
            awaitApplied(cluster, first.nodeId(), all);
            assertFalse(cluster.nodes.get(first.nodeId()).isLeader());
        }
    }

    @Test
    void anIsolatedLeaderStepsDownAndItsUncommittedWorkIsDiscarded() throws Exception {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode oldLeader = cluster.awaitLeader();
            proposeAll(oldLeader, List.of("shared"));

            cluster.network.isolate(oldLeader.nodeId(), cluster.ids);
            CompletableFuture<Object> stranded = propose(oldLeader, "never-committed");

            // CheckQuorum: cut off from the majority, the leader gives up within about one
            // election timeout (300 ms here), while still partitioned, and the waiting
            // proposal fails fast instead of hanging until the network heals.
            ExecutionException e = assertThrows(ExecutionException.class, () -> stranded.get(2, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof NotLeaderException);
            assertFalse(oldLeader.isLeader(), "an isolated leader must step down before the heal");

            List<String> majority = new ArrayList<>(cluster.ids);
            majority.remove(oldLeader.nodeId());
            RaftNode newLeader = cluster.awaitLeaderAmong(majority);
            proposeAll(newLeader, List.of("from-the-majority"));

            cluster.network.healAll();
            for (String id : cluster.ids) {
                awaitApplied(cluster, id, List.of("shared", "from-the-majority"));
            }
        }
    }

    @Test
    void aFollowerReturningFromAPartitionDoesNotDisruptTheLeader() throws Exception {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();
            long term = leader.currentTerm();
            String away = cluster.ids.stream().filter(id -> !id.equals(leader.nodeId())).findFirst().orElseThrow();

            cluster.network.isolate(away, cluster.ids);
            Thread.sleep(1_500); // five of its longest election timeouts
            proposeAll(leader, List.of("while-away"));
            cluster.network.healAll();
            awaitApplied(cluster, away, List.of("while-away"));
            Thread.sleep(500);

            // PreVote: the isolated node never won a pre-vote, so it never raised its
            // term, and coming back did not force a new election.
            assertTrue(leader.isLeader(), "the leader was disrupted");
            assertEquals(term, leader.currentTerm());
            assertEquals(term, cluster.nodes.get(away).currentTerm());
        }
    }

    @Test
    void aPreVoteChangesNothingAndIsRefusedWhileTheLeaderIsAlive() throws Exception {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();
            proposeAll(leader, List.of("x"));
            RaftNode follower = cluster.nodes.get(
                    cluster.ids.stream().filter(id -> !id.equals(leader.nodeId())).findFirst().orElseThrow());
            await(() -> follower.lastApplied() >= 2, "the follower to catch up");
            long term = follower.currentTerm();

            VoteResponse answer = follower.handleVote(
                    new VoteRequest(term + 1, "eager", follower.lastLogIndex(), term, true));

            assertFalse(answer.granted(), "the follower hears from a live leader");
            assertEquals(term, follower.currentTerm(), "a pre-vote must not change the term");
            assertTrue(leader.isLeader());
        }
    }

    @Test
    void aFollowerThatMissedEntriesCatchesUp() throws Exception {
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();
            String lagging = cluster.ids.stream().filter(id -> !id.equals(leader.nodeId())).findFirst().orElseThrow();
            cluster.network.isolate(lagging, cluster.ids);

            List<String> commands = commands("while-away-", 1_200); // more than one AppendEntries batch
            proposeAll(leader, commands);
            assertTrue(cluster.machines.get(lagging).applied().isEmpty());

            cluster.network.healAll();
            awaitApplied(cluster, lagging, commands);
        }
    }

    @Test
    void termsVotesAndTheLogSurviveARestartOfTheWholeCluster() throws Exception {
        long term;
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();
            proposeAll(leader, commands("durable-", 20));
            term = leader.currentTerm();
        }
        try (Cluster cluster = new Cluster(dir, 3)) {
            RaftNode leader = cluster.awaitLeader();
            assertTrue(leader.currentTerm() > term, "terms must never go backwards");
            // Fresh state machines are rebuilt from the replicated log.
            for (String id : cluster.ids) {
                awaitApplied(cluster, id, commands("durable-", 20));
            }
        }
    }

    // ---- randomized chaos ----------------------------------------------------------

    /**
     * Repeatedly crashes, restarts and isolates random nodes while proposing commands,
     * then checks Raft's safety properties:
     * <ul>
     *   <li>election safety: never two leaders in the same term;</li>
     *   <li>state machine safety: every node applies the same commands in the same order;</li>
     *   <li>durability: every command that was acknowledged is in the final log, once.</li>
     * </ul>
     */
    @Test
    void safetyHoldsThroughRandomCrashesAndPartitions() throws Exception {
        Random random = new Random(42);
        Map<Long, String> leaderOfTerm = new HashMap<>();
        Set<String> acknowledged = new HashSet<>();
        int counter = 0;
        try (Cluster cluster = new Cluster(dir, 5)) {
            for (int round = 0; round < 25; round++) {
                // Something goes wrong...
                String victim = cluster.ids.get(random.nextInt(cluster.ids.size()));
                switch (random.nextInt(3)) {
                    case 0 -> {
                        if (cluster.isUp(victim)) {
                            cluster.kill(victim);
                        } else {
                            cluster.start(victim);
                        }
                    }
                    case 1 -> cluster.network.isolate(victim, cluster.ids);
                    default -> cluster.network.healAll();
                }
                // ...while clients keep writing to whoever leads.
                long until = System.currentTimeMillis() + 250;
                while (System.currentTimeMillis() < until) {
                    for (RaftNode node : List.copyOf(cluster.nodes.values())) {
                        if (node.isLeader()) {
                            String previous = leaderOfTerm.putIfAbsent(node.currentTerm(), node.nodeId());
                            assertTrue(previous == null || previous.equals(node.nodeId()),
                                    "two leaders in term " + node.currentTerm());
                            String command = "c" + (counter++);
                            node.propose(command.getBytes(StandardCharsets.UTF_8)).thenAccept(r -> {
                                synchronized (acknowledged) {
                                    acknowledged.add(command);
                                }
                            });
                        }
                    }
                    Cluster.sleep(5);
                }
            }

            // Recover everything and let the cluster settle.
            cluster.network.healAll();
            for (String id : cluster.ids) {
                if (!cluster.isUp(id)) {
                    cluster.start(id);
                }
            }
            RaftNode leader = cluster.awaitLeader();
            propose(leader, "final").get(10, TimeUnit.SECONDS);
            await(() -> cluster.ids.stream().allMatch(id ->
                    cluster.machines.get(id).applied().contains("final")), "every node to apply everything");

            List<String> reference = cluster.machines.get(leader.nodeId()).applied();
            for (String id : cluster.ids) {
                await(() -> cluster.machines.get(id).applied().equals(reference), id + " to match the leader");
            }
            synchronized (acknowledged) {
                assertTrue(acknowledged.size() > 50, "chaos left too little progress: " + acknowledged.size());
                for (String command : acknowledged) {
                    assertTrue(reference.contains(command), "acknowledged command lost: " + command);
                }
            }
            assertEquals(new HashSet<>(reference).size(), reference.size(), "a command was applied twice");
        }
    }
}
