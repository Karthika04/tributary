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
