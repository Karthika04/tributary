package io.github.karthika04.tributary.chaos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * The failures the chaos runner injects. Each round picks one, injects it while
 * producers are writing, leaves it in place for a while, then heals everything with
 * {@link ChaosCluster#healAll()}.
 */
public enum Fault {

    /** kill -9 the leader: the cluster must elect another and lose nothing acknowledged. */
    KILL_LEADER,
    /** kill -9 a follower: the other two are still a majority, so writes go on. */
    KILL_FOLLOWER,
    /** kill -9 all three at once, then restart: acknowledged writes must survive on disk. */
    KILL_ALL,
    /**
     * Cut the leader off from both peers while clients can still reach it. It must not
     * acknowledge anything it cannot replicate; the majority side elects a new leader.
     */
    PARTITION_LEADER,
    /** Cut one follower off; it must catch up after the heal. */
    PARTITION_FOLLOWER,
    /**
     * Freeze the leader (SIGSTOP) long enough for the others to replace it, then wake
     * it: it comes back still believing it leads, and must step down.
     */
    PAUSE_LEADER,
    /** 50 to 250 ms of delay on every link between brokers. */
    SLOW_NETWORK;

    /** Injects this fault and returns a one-line description of what it did. */
    public String inject(ChaosCluster cluster, Random random) throws IOException, InterruptedException {
        String leader = cluster.leader();
        String target = leader != null ? leader : ChaosCluster.IDS.get(random.nextInt(3));
        String follower = randomOtherThan(target, random);
        return switch (this) {
            case KILL_LEADER -> {
                cluster.broker(target).kill();
                yield "killed " + target + (leader == null ? " (no leader seen)" : " (leader)");
            }
            case KILL_FOLLOWER -> {
                cluster.broker(follower).kill();
                yield "killed " + follower;
            }
            case KILL_ALL -> {
                for (String id : ChaosCluster.IDS) {
                    cluster.broker(id).kill();
                }
                yield "killed all";
            }
            case PARTITION_LEADER -> {
                cluster.isolate(target);
                yield "isolated " + target + (leader == null ? " (no leader seen)" : " (leader)");
            }
            case PARTITION_FOLLOWER -> {
                cluster.isolate(follower);
                yield "isolated " + follower;
            }
            case PAUSE_LEADER -> {
                cluster.broker(target).pause();
                yield "paused " + target + (leader == null ? " (no leader seen)" : " (leader)");
            }
            case SLOW_NETWORK -> {
                int ms = 50 + random.nextInt(201);
                cluster.setLatencyMs(ms);
                yield "added " + ms + " ms latency";
            }
        };
    }

    private static String randomOtherThan(String id, Random random) {
        List<String> others = new ArrayList<>(ChaosCluster.IDS);
        others.remove(id);
        return others.get(random.nextInt(others.size()));
    }
}
