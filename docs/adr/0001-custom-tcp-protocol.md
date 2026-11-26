# ADR 0001: Our own binary protocol over TCP instead of gRPC

- Status: accepted
- Date: 2026-11-26

## Context

Clients need a network protocol to create topics, produce and fetch records. The
original plan was gRPC with Protocol Buffers: popular, well documented, and it
generates client and server code from a schema.

## Decision

Tributary uses its own small binary protocol over plain TCP: length-prefixed frames,
a fixed request header (`apiKey`, `version`, `correlationId`, `clientId`), and one
body layout per request type. It is implemented with the JDK only.

## Reasons

1. **It is how Kafka works.** Kafka uses its own binary protocol over TCP, not gRPC,
   for the same reasons below. Building one shows the fundamentals that gRPC hides:
   framing a byte stream, matching responses to requests, versioning, and defending
   against malformed or hostile input.
2. **Control over the hot path.** Records travel as one compressed block, encoded
   straight into a buffer, with no extra object layer.
3. **Long polling stays simple.** A fetch can wait on the broker for new data with a
   plain blocking call on a virtual thread; nothing special is needed from the RPC
   framework.
4. **No dependencies** in the protocol, broker and client modules, so they are easy to
   build, audit and test anywhere.

## Consequences

- We had to write and test framing, the codec and error handling ourselves (18
  protocol tests cover round trips, truncation, padding, unknown versions and zip
  bombs).
- No automatic client generation for other languages. A non-Java client would follow
  the format in DESIGN.md section 5.
- gRPC still appears later: Apache Ratis, used for Raft replication between brokers
  (Phase 4), communicates over gRPC.
