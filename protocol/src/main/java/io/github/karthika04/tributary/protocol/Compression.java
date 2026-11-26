package io.github.karthika04.tributary.protocol;

/**
 * How a block of records is compressed on the wire. Text-like payloads such as JSON
 * often shrink 3 to 10 times with GZIP, at the cost of some CPU on each side.
 */
public enum Compression {
    NONE(0),
    GZIP(1);

    private final byte code;

    Compression(int code) {
        this.code = (byte) code;
    }

    public byte code() {
        return code;
    }

    public static Compression fromCode(byte code) {
        for (Compression c : values()) {
            if (c.code == code) {
                return c;
            }
        }
        throw new ProtocolException("unknown compression " + code);
    }
}
