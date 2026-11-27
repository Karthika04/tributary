# ADR 0005: Libraries only for measuring and observing, never in the data path

- Status: accepted, amended the same day (see "Amendment")
- Date: 2026-11-27

## Context

Until Phase 6, Tributary used no third-party library at runtime: storage, the wire
protocol, Raft, the broker and the client are written against the JDK alone. Phase 6
adds benchmarks, metrics, dashboards and structured logging, where the Java world has
standard tools: JMH for micro-benchmarks, HdrHistogram for latency percentiles,
Micrometer with Prometheus for metrics, SLF4J with Logback for logging.

Writing our own versions of these was possible: a timing loop, a sorted list of
samples, a hand-written Prometheus text endpoint, JSON logs through the JDK logger.

## Decision

- **The data path stays JDK-only.** Storage, protocol, Raft and the client add no
  dependency. The broker's code uses only two *facades*, thin interfaces whose
  implementation is chosen when the application starts: Micrometer's `MeterRegistry`
  for metrics and SLF4J's `MDC` for request IDs in logs. All logging still goes through
  the JDK's own `System.Logger`.
- **The broker application plugs in the implementations:** the Prometheus registry
  behind `/metrics`, Logback to write logs (text or JSON), and a bridge that sends
  `System.Logger` into SLF4J.
- **Measuring and observing use the standard libraries:** JMH and HdrHistogram in the
  `bench` module; Micrometer, SLF4J and Logback for metrics and logs.
- **JMH is used directly**, through its annotation processor and a small Gradle task,
  not through a Gradle plugin, so a new Gradle version cannot break the build.

## Reasons

- **The core is the interesting part to build;** the measuring tools are not. Owning
  storage, replication and the protocol is what the project is about. A hand-rolled
  benchmark harness or metrics registry would be worse than the standard one and would
  teach less.
- **These tools encode hard-won knowledge.** JMH handles JIT warm-up, dead-code
  elimination and forking; a timing loop gets all three wrong. HdrHistogram keeps
  exact percentiles in fixed memory. Micrometer is what Java services are measured
  with, and its Prometheus output works with Grafana as is.
- **They stay out of the data path.** No record is stored, replicated or delivered
  through library code, so the claims about the core (crash safety, Raft, the chaos
  results) do not depend on them. A broker request does touch the facades (a timer and
  a few map entries), which the benchmarks would show if it mattered.

## Amendment

The first version of this decision said the broker would add no runtime dependency at
all. That could not hold: metrics are recorded where requests are handled, which is in
the broker. Rather than hide that, the decision now draws the line where real Java
systems draw it: code depends on facades (as libraries depend on SLF4J rather than a
logging engine), and the application picks the engine.

## Consequences

- The build downloads a few libraries from Maven Central, listed in
  `gradle/libs.versions.toml`.
- The core remains easy to reason about and to explain line by line.
- Swapping a library (Micrometer for OpenTelemetry metrics, say) touches only the
  observing code.
