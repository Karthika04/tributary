package io.github.karthika04.tributary.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * The two values Raft must never forget across a crash: the current term and who this
 * node voted for in it. Forgetting a vote could let a node vote twice in one term, and
 * two leaders could be elected.
 *
 * <p>Saved by writing a temporary file, forcing it to disk, then renaming it over the
 * old one. A rename is atomic, so after a crash the file holds either the old values or
 * the new ones, never half of each.
 */
final class MetaStore {

    private final Path file;
    private long term;
    private String votedFor;

    MetaStore(Path dir) {
        this.file = dir.resolve("raft-meta");
        try {
            Files.createDirectories(dir);
            if (Files.exists(file)) {
                List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
                term = Long.parseLong(lines.get(0).trim());
                votedFor = lines.size() > 1 && !lines.get(1).isBlank() ? lines.get(1).trim() : null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
    }

    long term() {
        return term;
    }

    String votedFor() {
        return votedFor;
    }

    void save(long newTerm, String newVotedFor) {
        if (newTerm == term && java.util.Objects.equals(newVotedFor, votedFor)) {
            return;
        }
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        byte[] content = (newTerm + "\n" + (newVotedFor == null ? "" : newVotedFor) + "\n")
                .getBytes(StandardCharsets.UTF_8);
        try {
            try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                channel.write(ByteBuffer.wrap(content));
                channel.force(true);
            }
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write " + file, e);
        }
        term = newTerm;
        votedFor = newVotedFor;
    }
}
