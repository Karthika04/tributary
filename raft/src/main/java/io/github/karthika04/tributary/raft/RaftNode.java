package io.github.karthika04.tributary.raft;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One member of a Raft cluster, implemented from the Raft paper ("In Search of an
 * Understandable Consensus Algorithm", Ongaro and Ousterhout, 2014), section 5.
 *
 * <p><b>The idea.</b> A cluster of N nodes keeps one log of commands identical on every
 * node. One node is the <em>leader</em>; only it accepts new commands. It appends a
 * command to its log, copies it to the others, and once a <em>majority</em> has it on
 * disk the command is <em>committed</em>: it can never be lost or changed, even if
 * nodes crash, because any future leader must have every committed entry. Each node
 * then applies committed commands, in order, to its {@link StateMachine}.
 *
 * <p><b>Terms and elections (5.2).</b> Time is divided into numbered terms with at most
 * one leader each. A follower that hears nothing from a leader for a random election
 * timeout becomes a candidate: it starts a new term, votes for itself and asks the
 * others. A node votes at most once per term, and only for a candidate whose log is at
 * least as up to date as its own (5.4.1), so a winner always has every committed entry.
 * Any message with a newer term makes a node step down to follower.
 *
 * <p><b>Replication (5.3).</b> The leader sends AppendEntries to each follower: new
 * entries plus the index and term of the entry just before them. A follower accepts only
 * if it has that previous entry, which by induction means its whole log matches the
 * leader's up to there. On a mismatch the leader steps back and retries; the follower
 * deletes any conflicting entries it has (written by an old leader, never committed).
 *
 * <p><b>Commit rule (5.4.2).</b> A leader only counts replicas to commit entries from
 * its own term; older entries become committed along with them. On election, the leader
 * appends an empty no-op entry so this happens straight away.
 *
 * <p><b>Threads.</b> One lock guards all state. A ticker thread fires elections, one
 * replicator thread per peer sends AppendEntries, and an applier thread feeds committed
 * entries to the state machine. Network calls are always made without holding the lock.
 */
public final class RaftNode implements AutoCloseable {

    /** The three roles a node can be in. */
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    private static final System.Logger LOG = System.getLogger(RaftNode.class.getName());
    private static final long TICK_MS = 10;

    private final RaftConfig config;
    private final RaftTransport transport;
    private final StateMachine stateMachine;
    private final RaftLog log;
    private final MetaStore meta;

    private final ReentrantLock lock = new ReentrantLock();
    /** Signalled whenever there is something new to send or apply. */
    private final Condition changed = lock.newCondition();

    // Persistent state, mirrored in meta (Figure 2 of the paper).
    private long currentTerm;
    private String votedFor;

    // Volatile state.
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadlineNanos;

    // Leader-only state, reset on every election win.
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private long readyIndex = Long.MAX_VALUE;
    private final Map<Long, CompletableFuture<Object>> pending = new HashMap<>();

    private volatile boolean running = true;
    private final List<Thread> threads = new ArrayList<>();

    private RaftNode(RaftConfig config, RaftTransport transport, StateMachine stateMachine) {
        this.config = config;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.meta = new MetaStore(config.dataDir());
        this.log = new RaftLog(config.dataDir().resolve("log"));
        this.currentTerm = meta.term();
        this.votedFor = meta.votedFor();
    }

    /**
     * Starts a node. It begins as a follower and replays nothing on its own: committed
     * entries are re-applied to the (fresh) state machine as the node learns the commit
     * index from a leader, or by winning an election itself.
     */
    public static RaftNode start(RaftConfig config, RaftTransport transport, StateMachine stateMachine) {
        RaftNode node = new RaftNode(config, transport, stateMachine);
        node.lock.lock();
        try {
            node.resetElectionTimer();
        } finally {
            node.lock.unlock();
        }
        node.threads.add(Thread.ofPlatform().daemon(true).name("raft-" + config.nodeId() + "-ticker")
                .start(node::tickLoop));
        node.threads.add(Thread.ofPlatform().daemon(true).name("raft-" + config.nodeId() + "-applier")
                .start(node::applyLoop));
        for (String peer : config.peerIds()) {
            node.threads.add(Thread.ofVirtual().name("raft-" + config.nodeId() + "-to-" + peer)
                    .start(() -> node.replicateLoop(peer)));
        }
        return node;
    }

    // ---- proposing -----------------------------------------------------------------

    /**
     * Proposes a command. Completes with the state machine's result once the command is
     * committed and applied here, or exceptionally with {@link NotLeaderException} if
     * this node is not (or stops being) the leader. In that second case the command may
     * still have been committed by the next leader; callers that retry must tolerate
     * a duplicate.
     */
    public CompletableFuture<Object> propose(byte[] command) {
        if (command.length == 0) {
            throw new IllegalArgumentException("empty commands are reserved for no-ops");
        }
        lock.lock();
        try {
            if (role != Role.LEADER) {
                return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
            }
            long index = log.append(List.of(new LogEntry(currentTerm, command)));
            CompletableFuture<Object> future = new CompletableFuture<>();
            pending.put(index, future);
            advanceCommitIndex(); // a single-node cluster commits on its own
            changed.signalAll();
            return future;
        } finally {
            lock.unlock();
        }
    }

    // ---- elections (5.2) -------------------------------------------------------------

    private void tickLoop() {
        while (running) {
            lock.lock();
            try {
                if (role != Role.LEADER && System.nanoTime() >= electionDeadlineNanos) {
                    startElection();
                }
            } finally {
                lock.unlock();
            }
            sleep(TICK_MS);
        }
    }

    private void startElection() {
        currentTerm++;
        votedFor = config.nodeId();
        meta.save(currentTerm, votedFor);
        role = Role.CANDIDATE;
        leaderId = null;
        resetElectionTimer();
        long term = currentTerm;
        LOG.log(Level.DEBUG, config.nodeId() + " starts an election for term " + term);

        VoteRequest request = new VoteRequest(term, config.nodeId(), log.lastIndex(), log.lastTerm());
        int[] votes = {1}; // our own
        if (votes[0] >= config.majority()) {
            becomeLeader();
            return;
        }
        for (String peer : config.peerIds()) {
            Thread.ofVirtual().name("raft-" + config.nodeId() + "-vote-" + peer).start(() -> {
                VoteResponse response;
                try {
                    response = transport.requestVote(peer, request);
                } catch (IOException | RuntimeException e) {
                    return; // unreachable peers simply do not vote
                }
                lock.lock();
                try {
                    if (response.term() > currentTerm) {
                        stepDown(response.term());
                    } else if (role == Role.CANDIDATE && currentTerm == term && response.granted()) {
                        votes[0]++;
                        if (votes[0] >= config.majority()) {
                            becomeLeader();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            });
        }
    }

    /** Answers a candidate's RequestVote. */
    public VoteResponse handleVote(VoteRequest request) {
        lock.lock();
        try {
            checkRunning();
            if (request.term() > currentTerm) {
                stepDown(request.term());
            }
            boolean logOk = request.lastLogTerm() > log.lastTerm()
                    || (request.lastLogTerm() == log.lastTerm() && request.lastLogIndex() >= log.lastIndex());
            boolean grant = request.term() == currentTerm
                    && (votedFor == null || votedFor.equals(request.candidateId()))
                    && logOk;
            if (grant) {
                votedFor = request.candidateId();
                meta.save(currentTerm, votedFor); // must be on disk before we answer
                resetElectionTimer();
            }
            return new VoteResponse(currentTerm, grant);
        } finally {
            lock.unlock();
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = config.nodeId();
        for (String peer : config.peerIds()) {
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0L);
        }
        // A no-op from the new term lets earlier entries commit (5.4.2), and tells us when
        // our state machine is fully caught up and safe to read from.
        readyIndex = log.append(List.of(new LogEntry(currentTerm, new byte[0])));
        LOG.log(Level.INFO, config.nodeId() + " is the leader for term " + currentTerm);
        advanceCommitIndex();
        changed.signalAll();
    }

    /** Becomes a follower; with a newer term, also forgets this term's vote. */
    private void stepDown(long term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            meta.save(currentTerm, null);
        }
        if (role == Role.LEADER) {
            LOG.log(Level.INFO, config.nodeId() + " steps down in term " + currentTerm);
            failPending();
        }
        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            resetElectionTimer();
        }
        readyIndex = Long.MAX_VALUE;
    }

    private void resetElectionTimer() {
        long timeoutMs = ThreadLocalRandom.current().nextLong(config.electionTimeoutMinMs(),
                config.electionTimeoutMaxMs() + 1);
        electionDeadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    }

    // ---- replication (5.3) -------------------------------------------------------------

    private void replicateLoop(String peer) {
        long heartbeatNanos = TimeUnit.MILLISECONDS.toNanos(config.heartbeatIntervalMs());
        while (running) {
            AppendRequest request;
            lock.lock();
            try {
                if (role != Role.LEADER) {
                    awaitQuietly(heartbeatNanos);
                    continue;
                }
                long next = nextIndex.get(peer);
                long prev = next - 1;
                request = new AppendRequest(currentTerm, config.nodeId(), prev, log.termAt(prev),
                        log.entries(next, config.maxEntriesPerRequest(), config.maxBytesPerRequest()), commitIndex);
            } finally {
                lock.unlock();
            }

            AppendResponse response;
            try {
                response = transport.appendEntries(peer, request);
            } catch (IOException | RuntimeException e) {
                sleep(config.heartbeatIntervalMs()); // peer down or unreachable: keep trying
                continue;
            }

            lock.lock();
            try {
                if (response.term() > currentTerm) {
                    stepDown(response.term());
                    continue;
                }
                if (role != Role.LEADER || currentTerm != request.term()) {
                    continue; // the answer is about a leadership we no longer hold
                }
                if (response.success()) {
                    matchIndex.put(peer, Math.max(matchIndex.get(peer), response.matchIndex()));
                    nextIndex.put(peer, matchIndex.get(peer) + 1);
                    advanceCommitIndex();
                } else {
                    // Step back, using the follower's hint to skip a whole term at once.
                    long hinted = Math.max(1, response.matchIndex() + 1);
                    nextIndex.put(peer, Math.max(1, Math.min(nextIndex.get(peer) - 1, hinted)));
                    continue; // retry right away
                }
                if (nextIndex.get(peer) > log.lastIndex()) {
                    awaitQuietly(heartbeatNanos); // caught up: wait for new entries or the next heartbeat
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /** Answers the leader's AppendEntries. */
    public AppendResponse handleAppend(AppendRequest request) {
        lock.lock();
        try {
            checkRunning();
            if (request.term() < currentTerm) {
                return new AppendResponse(currentTerm, false, log.lastIndex()); // a stale leader
            }
            if (request.term() > currentTerm || role != Role.FOLLOWER) {
                stepDown(request.term());
            }
            leaderId = request.leaderId();
            resetElectionTimer();

            long prev = request.prevLogIndex();
            if (prev > log.lastIndex()) {
                return new AppendResponse(currentTerm, false, log.lastIndex()); // we are missing entries
            }
            if (log.termAt(prev) != request.prevLogTerm()) {
                // Conflict: hint the leader to retry from before this term's entries.
                return new AppendResponse(currentTerm, false, log.firstIndexOfTerm(prev) - 1);
            }

            long index = prev;
            List<LogEntry> entries = request.entries();
            for (int i = 0; i < entries.size(); i++) {
                index++;
                if (index <= log.lastIndex()) {
                    if (log.termAt(index) == entries.get(i).term()) {
                        continue; // already have it (a retried or delayed message)
                    }
                    if (index <= commitIndex) {
                        throw new IllegalStateException("refusing to delete committed entry " + index);
                    }
                    log.truncateFrom(index); // an uncommitted entry from an old leader
                }
                log.append(entries.subList(i, entries.size()));
                break;
            }
            long lastNew = prev + entries.size();
            if (request.leaderCommit() > commitIndex) {
                commitIndex = Math.min(request.leaderCommit(), lastNew);
                changed.signalAll();
            }
            return new AppendResponse(currentTerm, true, lastNew);
        } finally {
            lock.unlock();
        }
    }

    /** Leader only: commit the highest entry of this term that a majority stores (5.4.2). */
    private void advanceCommitIndex() {
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != currentTerm) {
                break; // older terms are only committed indirectly
            }
            int copies = 1; // the leader's own
            for (long match : matchIndex.values()) {
                if (match >= n) {
                    copies++;
                }
            }
            if (copies >= config.majority()) {
                commitIndex = n;
                changed.signalAll();
                return;
            }
        }
    }

    // ---- applying ----------------------------------------------------------------------

    private void applyLoop() {
        while (running) {
            long index;
            LogEntry entry;
            lock.lock();
            try {
                while (running && lastApplied >= commitIndex) {
                    awaitQuietly(TimeUnit.MILLISECONDS.toNanos(100));
                }
                if (!running) {
                    return;
                }
                index = lastApplied + 1;
                entry = log.entry(index);
            } finally {
                lock.unlock();
            }

            Object result = null;
            RuntimeException failure = null;
            if (!entry.isNoOp()) {
                try {
                    result = stateMachine.apply(index, entry.command());
                } catch (RuntimeException e) {
                    failure = e; // deterministic rejections fail the same way everywhere
                }
            }

            CompletableFuture<Object> future;
            lock.lock();
            try {
                lastApplied = index;
                future = pending.remove(index);
            } finally {
                lock.unlock();
            }
            if (future != null) {
                if (failure != null) {
                    future.completeExceptionally(failure);
                } else {
                    future.complete(result);
                }
            }
        }
    }

    private void failPending() {
        for (CompletableFuture<Object> f : pending.values()) {
            f.completeExceptionally(new NotLeaderException(null));
        }
        pending.clear();
    }

    // ---- status ------------------------------------------------------------------------

    public String nodeId() {
        return config.nodeId();
    }

    public boolean isLeader() {
        lock.lock();
        try {
            return role == Role.LEADER;
        } finally {
            lock.unlock();
        }
    }

    /**
     * True once this leader has applied everything committed before it took over, so
     * reads from its state machine are up to date.
     */
    public boolean isReadyLeader() {
        lock.lock();
        try {
            return role == Role.LEADER && lastApplied >= readyIndex;
        } finally {
            lock.unlock();
        }
    }

    /** The leader this node currently follows, or null during an election. */
    public String leaderId() {
        lock.lock();
        try {
            return leaderId;
        } finally {
            lock.unlock();
        }
    }

    public Role role() {
        lock.lock();
        try {
            return role;
        } finally {
            lock.unlock();
        }
    }

    public long currentTerm() {
        lock.lock();
        try {
            return currentTerm;
        } finally {
            lock.unlock();
        }
    }

    public long commitIndex() {
        lock.lock();
        try {
            return commitIndex;
        } finally {
            lock.unlock();
        }
    }

    public long lastApplied() {
        lock.lock();
        try {
            return lastApplied;
        } finally {
            lock.unlock();
        }
    }

    public long lastLogIndex() {
        lock.lock();
        try {
            return log.lastIndex();
        } finally {
            lock.unlock();
        }
    }

    // ---- shutdown ----------------------------------------------------------------------

    /**
     * Stops the node. Threads are signalled and joined, never interrupted, because an
     * interrupt during a file write would close the log file.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (!running) {
                return;
            }
            running = false;
            role = Role.FOLLOWER;
            failPending();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        for (Thread t : threads) {
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.close();
    }

    private void checkRunning() {
        if (!running) {
            throw new IllegalStateException("raft node " + config.nodeId() + " is stopped");
        }
    }

    private void awaitQuietly(long nanos) {
        try {
            changed.awaitNanos(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
