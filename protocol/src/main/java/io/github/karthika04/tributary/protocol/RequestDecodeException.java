package io.github.karthika04.tributary.protocol;

/**
 * A request whose header was readable but whose body was not (or whose version is
 * not supported). Because the header was read, the broker still knows which request
 * to answer, so it can send an error back instead of dropping the connection.
 */
public class RequestDecodeException extends ProtocolException {

    private final int correlationId;
    private final ApiKey apiKey;
    private final ErrorCode error;

    public RequestDecodeException(int correlationId, ApiKey apiKey, ErrorCode error, String message,
                                  Throwable cause) {
        super(message, cause);
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.error = error;
    }

    public int correlationId() {
        return correlationId;
    }

    public ApiKey apiKey() {
        return apiKey;
    }

    public ErrorCode error() {
        return error;
    }
}
