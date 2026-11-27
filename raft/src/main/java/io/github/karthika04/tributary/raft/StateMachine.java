package io.github.karthika04.tributary.raft;

/**
 * What the replicated log drives. Every node applies the same commands in the same
 * order, so if {@link #apply} is deterministic, every node ends up in the same state.
 */
public interface StateMachine {

    /**
     * Applies one committed command. Called exactly once per entry, in index order.
     *
     * @return a result for whoever proposed the command (for example, an offset)
     * @throws RuntimeException to reject the command; it must fail the same way on
     *                          every node
     */
    Object apply(long index, byte[] command);
}
