package io.github.karthika04.tributary.protocol;

/**
 * Every way a request can fail, as sent on the wire.
 *
 * <p>{@code retriable} tells a client whether trying again later could succeed. A
 * producer retries those automatically; the others are reported to the caller.
 */
public enum ErrorCode {
    NONE(0, false),
    UNKNOWN_TOPIC(1, false),
    UNKNOWN_PARTITION(2, false),
    OFFSET_OUT_OF_RANGE(3, false),
    INVALID_REQUEST(4, false),
    RECORD_TOO_LARGE(5, false),
    TOPIC_ALREADY_EXISTS(6, false),
    INVALID_TOPIC(7, false),
    CORRUPT_MESSAGE(8, false),
    UNSUPPORTED_VERSION(9, false),
    BROKER_SHUTTING_DOWN(10, true),
    INTERNAL_ERROR(11, true),
    /** Not sent by the broker: the client lost its connection or timed out. */
    NETWORK_ERROR(12, true),
    /** The group does not know this member: its session expired, or the broker restarted. Join again. */
    UNKNOWN_MEMBER(13, false),
    /** The group's partitions were reassigned since this member last joined. Join again. */
    REBALANCE_IN_PROGRESS(14, false),
    /** Invalid group id, or settings that clash with the group's existing members. */
    INVALID_GROUP(15, false);

    private final short code;
    private final boolean retriable;

    ErrorCode(int code, boolean retriable) {
        this.code = (short) code;
        this.retriable = retriable;
    }

    public short code() {
        return code;
    }

    public boolean retriable() {
        return retriable;
    }

    public static ErrorCode fromCode(short code) {
        for (ErrorCode error : values()) {
            if (error.code == code) {
                return error;
            }
        }
        throw new ProtocolException("unknown error code " + code);
    }
}
