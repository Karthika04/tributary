package io.github.karthika04.tributary.raft;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** A state machine that just remembers every command it applied, in order. */
final class RecordingStateMachine implements StateMachine {

    private final List<String> applied = new ArrayList<>();
    private long lastIndex;

    @Override
    public synchronized Object apply(long index, byte[] command) {
        if (index <= lastIndex) {
            throw new AssertionError("index " + index + " applied after " + lastIndex);
        }
        lastIndex = index;
        String text = new String(command, StandardCharsets.UTF_8);
        if (text.startsWith("reject")) {
            throw new IllegalArgumentException("rejected: " + text);
        }
        applied.add(text);
        return "ok:" + text;
    }

    synchronized List<String> applied() {
        return List.copyOf(applied);
    }
}
