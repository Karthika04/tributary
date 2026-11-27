package io.github.karthika04.tributary.raft;

import java.util.Arrays;

/**
 * One entry in the replicated log: a command for the state machine, stamped with the
 * term of the leader that created it. An empty command is a no-op that a new leader
 * appends to commit everything before it.
 */
public record LogEntry(long term, byte[] command) {

    public LogEntry {
        if (term < 1) {
            throw new IllegalArgumentException("term must be >= 1");
        }
        if (command == null) {
            throw new IllegalArgumentException("command must not be null");
        }
    }

    boolean isNoOp() {
        return command.length == 0;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LogEntry other && term == other.term && Arrays.equals(command, other.command);
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(term) + Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return "LogEntry[term=" + term + ", bytes=" + command.length + "]";
    }
}
