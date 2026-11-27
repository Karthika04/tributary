package io.github.karthika04.tributary.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.TopicPartition;
import io.github.karthika04.tributary.storage.LogConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GroupCoordinatorTest {

    private static final int SESSION_MS = 10_000;

    @TempDir
    Path dataDir;

    private final TestClock clock = new TestClock(1_000_000);
    private TopicManager topics;
    private GroupCoordinator groups;

    private GroupCoordinator start() {
        topics = new TopicManager(dataDir, LogConfig.defaults());
        OffsetStore offsets = new OffsetStore(topics.internalTopic(OffsetStore.TOPIC), clock);
        groups = new GroupCoordinator(topics, offsets, new LocalWrites(topics, offsets), clock);
        return groups;
    }

    @AfterEach
    void stop() {
        if (topics != null) {
            topics.close();
        }
    }

    private Response.JoinGroup join(String memberId, String... subscribed) {
        return groups.join("billing", memberId, List.of(subscribed), SESSION_MS, Assignors.RANGE);
    }

    private static ErrorCode errorOf(Runnable action) {
        return assertThrows(BrokerException.class, action::run).error();
    }

    private static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    @Test
    void aLoneMemberGetsEveryPartition() {
        start();
        topics.createTopic("orders", 3);

        Response.JoinGroup joined = join(null, "orders");

        assertTrue(joined.memberId().startsWith("billing-"));
        assertEquals(1, joined.generation());
        assertEquals(List.of(tp("orders", 0), tp("orders", 1), tp("orders", 2)), joined.assignment());
    }

    @Test
    void aSecondMemberTriggersARebalanceAndTheFirstLearnsByHeartbeat() {
        start();
        topics.createTopic("orders", 4);
        Response.JoinGroup first = join(null, "orders");

        Response.JoinGroup second = join(null, "orders");

        assertEquals(2, second.generation());
        assertEquals(2, second.assignment().size());
        // The first member still thinks it is in generation 1 until its next heartbeat...
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS,
                errorOf(() -> groups.heartbeat("billing", first.memberId(), first.generation())));
        // ...then it re-joins with its id and gets its half, without another rebalance.
        Response.JoinGroup rejoined = join(first.memberId(), "orders");
        assertEquals(2, rejoined.generation());
        assertEquals(2, rejoined.assignment().size());
        groups.heartbeat("billing", first.memberId(), 2); // now up to date

        Set<TopicPartition> all = new HashSet<>(rejoined.assignment());
        second.assignment().forEach(p -> assertTrue(all.add(p), "two owners for " + p));
        assertEquals(4, all.size());
    }

    @Test
    void leavingHandsPartitionsBackImmediately() {
        start();
        topics.createTopic("orders", 2);
        Response.JoinGroup a = join(null, "orders");
        Response.JoinGroup b = join(null, "orders");

        groups.leave("billing", b.memberId());

        Response.JoinGroup rejoined = join(a.memberId(), "orders");
        assertEquals(2, rejoined.assignment().size());
        assertEquals(ErrorCode.UNKNOWN_MEMBER, errorOf(() -> groups.heartbeat("billing", b.memberId(), 3)));
    }

    @Test
    void aSilentMemberIsRemovedAfterItsSessionTimeout() {
        start();
        topics.createTopic("orders", 2);
        Response.JoinGroup alive = join(null, "orders");
        Response.JoinGroup silent = join(null, "orders");
        join(alive.memberId(), "orders");

        clock.advance(Duration.ofMillis(SESSION_MS / 2));
        groups.heartbeat("billing", alive.memberId(), 2);
        assertEquals(0, groups.expireMembers()); // nobody has been silent long enough yet

        clock.advance(Duration.ofMillis(SESSION_MS / 2 + 1));
        assertEquals(1, groups.expireMembers()); // "silent" missed its whole session

        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS,
                errorOf(() -> groups.heartbeat("billing", alive.memberId(), 2)));
        assertEquals(2, join(alive.memberId(), "orders").assignment().size());
        assertEquals(ErrorCode.UNKNOWN_MEMBER,
                errorOf(() -> groups.heartbeat("billing", silent.memberId(), 3)));
    }

    @Test
    void onlyThePartitionsOwnerCanCommitIt() {
        start();
        topics.createTopic("orders", 2);
        Response.JoinGroup a = join(null, "orders");
        Response.JoinGroup b = join(null, "orders");
        Response.JoinGroup aNow = join(a.memberId(), "orders");
        TopicPartition aOwns = aNow.assignment().get(0);
        TopicPartition bOwns = b.assignment().get(0);

        List<TopicPartition> rejected = groups.commit("billing", a.memberId(), List.of(
                new PartitionOffset(aOwns.topic(), aOwns.partition(), 10),
                new PartitionOffset(bOwns.topic(), bOwns.partition(), 99)));

        assertEquals(List.of(bOwns), rejected);
        List<PartitionOffset> saved = groups.fetchOffsets("billing", List.of(aOwns, bOwns));
        assertEquals(10, saved.get(0).offset());
        assertEquals(-1, saved.get(1).offset()); // the zombie write was refused
    }

    @Test
    void committedOffsetsSurviveARestart() {
        start();
        topics.createTopic("orders", 1);
        Response.JoinGroup a = join(null, "orders");
        groups.commit("billing", a.memberId(), List.of(new PartitionOffset("orders", 0, 5)));
        groups.commit("billing", a.memberId(), List.of(new PartitionOffset("orders", 0, 8)));
        topics.close();

        start(); // a brand-new coordinator that only has the files on disk

        assertEquals(8, groups.fetchOffsets("billing", List.of(tp("orders", 0))).get(0).offset());
        assertEquals(-1, groups.fetchOffsets("other-group", List.of(tp("orders", 0))).get(0).offset());
        // Membership is not durable: the old member must join again.
        assertEquals(ErrorCode.UNKNOWN_MEMBER, errorOf(() -> groups.heartbeat("billing", a.memberId(), 1)));
    }

    @Test
    void separateGroupsEachGetEveryPartition() {
        start();
        topics.createTopic("orders", 2);

        Response.JoinGroup billing = groups.join("billing", null, List.of("orders"), SESSION_MS, Assignors.RANGE);
        Response.JoinGroup shipping = groups.join("shipping", null, List.of("orders"), SESSION_MS, Assignors.RANGE);

        assertEquals(2, billing.assignment().size());
        assertEquals(2, shipping.assignment().size());
        assertNotEquals(billing.memberId(), shipping.memberId());
    }

    @Test
    void rejectsBadJoins() {
        start();
        topics.createTopic("orders", 1);
        join(null, "orders");

        assertEquals(ErrorCode.UNKNOWN_TOPIC, errorOf(() -> join(null, "missing")));
        assertEquals(ErrorCode.INVALID_GROUP, errorOf(() -> join(null)));
        assertEquals(ErrorCode.UNKNOWN_MEMBER, errorOf(() -> join("billing-nope", "orders")));
        assertEquals(ErrorCode.INVALID_GROUP, errorOf(
                () -> groups.join("billing", null, List.of("orders"), SESSION_MS, Assignors.ROUND_ROBIN)));
        assertEquals(ErrorCode.INVALID_GROUP, errorOf(
                () -> groups.join("bad/group", null, List.of("orders"), SESSION_MS, Assignors.RANGE)));
        assertEquals(ErrorCode.INVALID_GROUP, errorOf(
                () -> groups.join("g", null, List.of("orders"), 10, Assignors.RANGE)));
        assertEquals(ErrorCode.INVALID_GROUP, errorOf(
                () -> groups.join("g", null, List.of("orders"), SESSION_MS, "fastest")));
    }

    @Test
    void theInternalTopicIsHiddenAndReserved() {
        start();
        topics.createTopic("orders", 1);

        assertEquals(List.of(new Response.TopicMetadata("orders", 1)), topics.metadata());
        assertEquals(ErrorCode.INVALID_TOPIC, errorOf(() -> topics.createTopic("__mine", 1)));
    }

    @Test
    void changingSubscriptionsRebalances() {
        start();
        topics.createTopic("orders", 1);
        topics.createTopic("payments", 1);
        Response.JoinGroup first = join(null, "orders");

        Response.JoinGroup changed = join(first.memberId(), "orders", "payments");

        assertEquals(2, changed.generation());
        assertEquals(Map.of(first.memberId(), List.of(tp("orders", 0), tp("payments", 0))),
                groups.assignments("billing"));
    }
}
