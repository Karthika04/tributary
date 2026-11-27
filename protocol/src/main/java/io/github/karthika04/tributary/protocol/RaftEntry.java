package io.github.karthika04.tributary.protocol;

import java.util.Arrays;

/** A Raft log entry on the wire, between brokers. */
public record RaftEntry(long term, byte[] command) {

    @Override
    public boolean equals(Object o) {
        return o instanceof RaftEntry other && term == other.term && Arrays.equals(command, other.command);
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(term) + Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return "RaftEntry[term=" + term + ", bytes=" + command.length + "]";
    }
}
