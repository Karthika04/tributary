package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.ErrorCode;

/** A request failed for a reason the client should be told about, with its error code. */
class BrokerException extends RuntimeException {

    private final ErrorCode error;

    BrokerException(ErrorCode error, String message) {
        super(message);
        this.error = error;
    }

    ErrorCode error() {
        return error;
    }
}
