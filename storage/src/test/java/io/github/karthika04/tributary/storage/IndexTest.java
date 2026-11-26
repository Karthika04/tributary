package io.github.karthika04.tributary.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexTest {

    @TempDir
    Path dir;

    @Test
    void findsThePositionOfEveryEntry() throws IOException {
        int count = 100_000;
        try (Index index = Index.openForAppend(dir.resolve("i.index"), count * Index.ENTRY_SIZE)) {
            for (int i = 0; i < count; i++) {
                index.append(i, i * 37);
            }
            for (int i = 0; i < count; i++) {
                assertEquals(i * 37, index.positionOf(i));
            }
            assertTrue(index.isFull());
        }
    }

    @Test
    void cleanCloseShrinksTheFileAndReopenKeepsEntries() throws IOException {
        Path path = dir.resolve("i.index");
        try (Index index = Index.openForAppend(path, 1024)) {
            assertEquals(1024, Files.size(path)); // pre-sized while active
            index.append(0, 0);
            index.append(1, 50);
            index.append(2, 120);
        }
        assertEquals(3 * Index.ENTRY_SIZE, Files.size(path));

        try (Index index = Index.openReadOnly(path)) {
            assertEquals(3, index.entries());
            assertEquals(120, index.positionOf(2));
        }
        try (Index index = Index.openForAppend(path, 1024)) {
            assertEquals(3, index.entries());
            index.append(3, 200);
            assertEquals(200, index.positionOf(3));
        }
    }

    @Test
    void readOnlyIndexRejectsAppends() throws IOException {
        Path path = dir.resolve("i.index");
        try (Index index = Index.openForAppend(path, 64)) {
            index.append(0, 0);
        }
        try (Index index = Index.openReadOnly(path)) {
            assertThrows(IllegalStateException.class, () -> index.append(1, 10));
        }
    }

    @Test
    void rejectsEntriesOutOfOrder() throws IOException {
        try (Index index = Index.openForAppend(dir.resolve("i.index"), 64)) {
            index.append(0, 0);
            assertThrows(IllegalArgumentException.class, () -> index.append(5, 10));
        }
    }

    @Test
    void rejectsAppendWhenFull() throws IOException {
        try (Index index = Index.openForAppend(dir.resolve("i.index"), 2 * Index.ENTRY_SIZE)) {
            index.append(0, 0);
            assertFalse(index.isFull());
            index.append(1, 10);
            assertTrue(index.isFull());
            assertThrows(IllegalStateException.class, () -> index.append(2, 20));
        }
    }

    @Test
    void lookupOutsideTheIndexFails() throws IOException {
        try (Index index = Index.openForAppend(dir.resolve("i.index"), 64)) {
            index.append(0, 0);
            assertThrows(IndexOutOfBoundsException.class, () -> index.positionOf(1));
            assertThrows(IndexOutOfBoundsException.class, () -> index.positionOf(-1));
        }
    }

    @Test
    void readOnlyOpenRejectsATornFile() throws IOException {
        Path path = dir.resolve("i.index");
        Files.write(path, new byte[Index.ENTRY_SIZE + 3]);

        assertThrows(CorruptRecordException.class, () -> Index.openReadOnly(path));
    }
}
