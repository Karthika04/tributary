package io.github.karthika04.tributary.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class BrokerMainTest {

    private static final String CLUSTER = "1=localhost:9292, 2=localhost:9293,3=localhost:9294";

    @Test
    void aSingleBrokerByDefault() {
        BrokerConfig config = BrokerMain.parse(new String[] {"--port", "9300"});
        assertEquals(9300, config.port());
        assertFalse(config.isClustered());
        assertNull(BrokerMain.parse(new String[] {"--help"}));
    }

    @Test
    void aClusterMemberListensOnItsOwnEntryUnlessTold() {
        BrokerConfig config = BrokerMain.parse(new String[] {"--node-id", "2", "--cluster", CLUSTER});
        assertTrue(config.isClustered());
        assertEquals("2", config.nodeId());
        assertEquals(9293, config.port());
        assertEquals(Map.of("1", "localhost:9292", "2", "localhost:9293", "3", "localhost:9294"),
                config.clusterNodes());

        BrokerConfig explicit = BrokerMain.parse(new String[] {"--node-id", "2", "--cluster", CLUSTER,
                "--port", "7000"});
        assertEquals(7000, explicit.port());
    }

    @Test
    void peersCanBeReachedAtOtherAddresses() {
        BrokerConfig config = BrokerMain.parse(new String[] {"--node-id", "1", "--cluster", CLUSTER,
                "--peer-addresses", "2=localhost:7002"});
        assertEquals("localhost:7002", config.peerAddress("2"));
        assertEquals("localhost:9294", config.peerAddress("3")); // not overridden
        assertEquals("localhost:9293", config.clusterNodes().get("2")); // still what clients are told
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.parse(new String[] {"--node-id", "1",
                "--cluster", CLUSTER, "--peer-addresses", "1=localhost:7001"})); // itself
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.parse(new String[] {"--node-id", "1",
                "--cluster", CLUSTER, "--peer-addresses", "9=localhost:7009"})); // not a member
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.parse(new String[] {
                "--peer-addresses", "2=localhost:7002"})); // no cluster
    }

    @Test
    void metricsAreServedOnlyWhenAskedFor() {
        assertEquals(-1, BrokerMain.metricsPort(new String[] {"--port", "9300"}));
        String[] args = {"--port", "9300", "--metrics-port", "9400"};
        assertEquals(9400, BrokerMain.metricsPort(args));
        assertEquals(9300, BrokerMain.parse(args).port()); // the broker's own flags still parse
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.metricsPort(new String[] {"--metrics-port", "x"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.metricsPort(new String[] {"--metrics-port", "70000"}));
    }

    @Test
    void rejectsBadClusterSettings() {
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.parse(new String[] {"--cluster", CLUSTER}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.parse(new String[] {"--node-id", "1"}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.parse(new String[] {"--node-id", "9", "--cluster", CLUSTER}));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.parseCluster("1=localhost:9292,2=localhost:9293"));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.parseCluster("1=localhost:9292,1=localhost:9293,3=localhost:9294"));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.parseCluster("1=localhost,2=localhost:9293,3=localhost:9294"));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.parseCluster("1=localhost:99999,2=localhost:9293,3=localhost:9294"));
    }
}
