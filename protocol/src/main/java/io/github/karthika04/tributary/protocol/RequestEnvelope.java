package io.github.karthika04.tributary.protocol;

/**
 * A request plus its header. The client picks {@code correlationId} and the broker
 * echoes it in the response, so the client can match answers to questions.
 */
public record RequestEnvelope(int correlationId, String clientId, Request request) {
}
