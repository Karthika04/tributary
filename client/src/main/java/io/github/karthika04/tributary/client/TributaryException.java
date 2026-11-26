package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.ErrorCode;

/** A request to the broker failed. {@link #error()} says why. */
public class TributaryException extends RuntimeException {

    private final ErrorCode error;

    public TributaryException(ErrorCode error, String message) {
        super(error + ": " + message);
        this.error = error;
    }

    public TributaryException(ErrorCode error, String message, Throwable cause) {
        super(error + ": " + message, cause);
        this.error = error;
    }

    public ErrorCode error() {
        return error;
    }

    /** True if the same request might succeed if tried again later. */
    public boolean retriable() {
        return error.retriable();
    }
}
