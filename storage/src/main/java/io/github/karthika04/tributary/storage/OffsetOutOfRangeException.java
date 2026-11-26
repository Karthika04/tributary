package io.github.karthika04.tributary.storage;

/**
 * Thrown when someone asks for an offset the log does not hold: either it has not
 * been written yet, or retention already deleted it.
 */
public class OffsetOutOfRangeException extends RuntimeException {

    private final long requested;
    private final long startOffset;
    private final long endOffset;

    public OffsetOutOfRangeException(long requested, long startOffset, long endOffset) {
        super("offset " + requested + " is out of range; the log holds offsets ["
                + startOffset + ", " + endOffset + ")");
        this.requested = requested;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public long requested() {
        return requested;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }
}
