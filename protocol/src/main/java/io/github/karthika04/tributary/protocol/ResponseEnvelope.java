package io.github.karthika04.tributary.protocol;

/**
 * A response plus its header. Exactly one of these holds: {@code error} is
 * {@link ErrorCode#NONE} and {@code body} is set, or {@code error} says what went wrong,
 * {@code errorMessage} explains it, and {@code body} is null.
 */
public record ResponseEnvelope(int correlationId, ApiKey apiKey, ErrorCode error, String errorMessage,
                               Response body) {

    public static ResponseEnvelope success(int correlationId, Response body) {
        return new ResponseEnvelope(correlationId, body.apiKey(), ErrorCode.NONE, null, body);
    }

    public static ResponseEnvelope failure(int correlationId, ApiKey apiKey, ErrorCode error, String message) {
        if (error == ErrorCode.NONE) {
            throw new IllegalArgumentException("a failure needs an error code");
        }
        return new ResponseEnvelope(correlationId, apiKey, error, message, null);
    }
}
