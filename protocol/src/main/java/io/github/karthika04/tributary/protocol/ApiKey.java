package io.github.karthika04.tributary.protocol;

/**
 * Which operation a request asks for. The number is what goes on the wire, so it must
 * never change once released; new operations get new numbers.
 */
public enum ApiKey {
    CREATE_TOPIC(0),
    METADATA(1),
    PRODUCE(2),
    FETCH(3),
    LIST_OFFSETS(4),
    JOIN_GROUP(5),
    HEARTBEAT(6),
    LEAVE_GROUP(7),
    COMMIT_OFFSETS(8),
    FETCH_OFFSETS(9),
    RAFT_VOTE(10),
    RAFT_APPEND(11),
    /** Fetch from many partitions at once; answered as soon as any of them has data. */
    MULTI_FETCH(12);

    private final short code;

    ApiKey(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    public static ApiKey fromCode(short code) {
        for (ApiKey key : values()) {
            if (key.code == code) {
                return key;
            }
        }
        throw new ProtocolException("unknown api key " + code);
    }
}
