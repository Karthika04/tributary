package io.github.karthika04.tributary.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StoreTest {

    private static final int MAX_FRAME = 1024 * 1024;

    @TempDir
    Path dir;

    private static ByteBuffer frame(long offset, String value) {
        return RecordCodec.encode(new Record(offset, 0, null, value.getBytes(UTF_8)));
    }

    @Test
    void appendReturnsWhereEachFrameStarts() throws IOException {
        try (Store store = Store.open(dir.resolve("s.log"))) {
            ByteBuffer first = frame(0, "a");
            int firstSize = first.remaining();

            assertEquals(0, store.append(first));
            assertEquals(firstSize, store.append(frame(1, "bb")));
            assertEquals(firstSize + RecordCodec.FRAME_OVERHEAD + 2, store.size());
        }
    }

    @Test
    void readsBackTheExactFrame() throws IOException {
        try (Store store = Store.open(dir.resolve("s.log"))) {
            store.append(frame(0, "first"));
            long position = store.append(frame(1, "second"));

            Record record = RecordCodec.decode(store.readFrame(position, MAX_FRAME));

            assertEquals(1, record.offset());
            assertEquals("second", new String(record.value(), UTF_8));
        }
    }

    @Test
    void keepsDataAcrossReopen() throws IOException {
        Path path = dir.resolve("s.log");
        long position;
        try (Store store = Store.open(path)) {
            position = store.append(frame(0, "persisted"));
        }
        try (Store store = Store.open(path)) {
            assertEquals(Files.size(path), store.size());
            assertEquals("persisted",
                    new String(RecordCodec.decode(store.readFrame(position, MAX_FRAME)).value(), UTF_8));
        }
    }

    @Test
    void reportsAFrameThatIsCutOffAtTheEndOfTheFile() throws IOException {
        try (Store store = Store.open(dir.resolve("s.log"))) {
            ByteBuffer full = frame(0, "this will be cut");
            store.append(full.slice(0, full.remaining() - 3));

            assertThrows(CorruptRecordException.class, () -> store.readFrame(0, MAX_FRAME));
        }
    }

    @Test
    void refusesAHugeLengthFieldInsteadOfAllocatingIt() throws IOException {
        try (Store store = Store.open(dir.resolve("s.log"))) {
            ByteBuffer garbage = ByteBuffer.allocate(64);
            garbage.putInt(0, Integer.MAX_VALUE);
            store.append(garbage);

            assertThrows(CorruptRecordException.class, () -> store.readFrame(0, MAX_FRAME));
        }
    }

    @Test
    void truncateDropsTheTail() throws IOException {
        try (Store store = Store.open(dir.resolve("s.log"))) {
            store.append(frame(0, "keep"));
            long cut = store.size();
            store.append(frame(1, "drop"));

            store.truncateTo(cut);

            assertEquals(cut, store.size());
            assertThrows(CorruptRecordException.class, () -> store.readFrame(cut, MAX_FRAME));
        }
    }
}
