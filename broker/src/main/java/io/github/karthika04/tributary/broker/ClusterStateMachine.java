package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.raft.StateMachine;

/**
 * Connects Raft to the broker: every committed command is applied to this broker's own
 * topics and offsets. All brokers apply the same commands in the same order, so they all
 * hold the same data.
 */
final class ClusterStateMachine implements StateMachine {

    private final LocalWrites local;

    ClusterStateMachine(LocalWrites local) {
        this.local = local;
    }

    @Override
    public Object apply(long index, byte[] command) {
        return Commands.apply(command, local);
    }
}
