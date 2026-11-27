# ADR 0004: Chaos tests run brokers as plain processes behind our own TCP proxy

- Status: accepted
- Date: 2026-11-26

## Context

Phase 5 tests the cluster under failure: crashed brokers, network partitions, frozen
processes, slow links. The usual tools for this in Java are Testcontainers (each broker
in a Docker container) and Toxiproxy (a proxy whose links can be cut or slowed through
an API), and Jepsen for the most thorough work.

We had three constraints:

- The tests must run on every build: on GitHub Actions and on a developer's laptop,
  with no setup beyond a JDK.
- Hundreds of rounds must be cheap enough to run often.
- The project has stayed JDK-only so far (no third-party runtime libraries), which
  keeps every part of it explainable.

## Decision

- **Each broker runs as a separate JVM process**, started from the test's own class
  path. A crash is `kill -9`; a frozen machine is `SIGSTOP`/`SIGCONT`.
- **Network faults go through our own `ChaosProxy`** (about 150 lines): one per one-way
  link between brokers, able to cut a link (black-holing new connections, so requests
  hang the way they do on a dead network) or add latency. Brokers reach their peers
  through these proxies via the new `--peer-addresses` flag; clients connect directly.
- **A checker in the style of Jepsen's queue checkers** decides pass or fail: no lost
  acknowledged writes, acknowledged offsets hold the right records, per-producer order,
  no foreign values; duplicates are counted, not failed (at-least-once). Raft logs are
  compared on disk at the end.

## Reasons

- **Real failures, not simulated ones.** `kill -9` on a real process tests what a
  simulated network cannot: the operating system tearing down sockets, the JVM dying
  without a shutdown hook, recovery from what is actually on disk.
- **Runs anywhere a JDK runs**, including CI, with no Docker daemon. A whole round
  (start, fault, heal, check) takes about 6 seconds, so 500 rounds is under an hour.
- **Directional partitions for free.** One proxy per one-way link means any partition
  shape, including one-way ones, is a matter of which proxies to cut.
- **It found real bugs** in its first runs (CheckQuorum and PreVote; see DESIGN.md
  section 9.2), which is the test of a test harness.

## Consequences

- **No disk faults.** Docker and Toxiproxy cannot fake a slow or full disk either, but
  a FUSE filesystem or `dm-delay` could. The group-commit bug (a slow `fsync` starving
  heartbeats) was found on a Mac and reproduced by a delay in code, and the 500-round
  run's only outliers match the signature of a stalled shared disk (DESIGN.md section
  10.4). A slow-disk fault is a candidate for later; it would need a hook in the
  storage layer or a special filesystem.
- **Everything on one machine.** Timing differs from a real network. `SLOW_NETWORK` and
  black-holed links cover part of that gap.
- **Clock faults are not tested.** Raft does not rely on synchronized clocks for safety,
  only for timeouts, so this matters less than for systems that do.
- **The upgrade path is clear.** Docker images and a Compose cluster come in Phase 7;
  the same faults could then be driven through Toxiproxy or Jepsen against containers.
