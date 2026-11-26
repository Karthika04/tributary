package io.github.karthika04.tributary.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class FramesTest {

    @Test
    void readsBackFramesInOrder() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Frames.write(out, new byte[] {1, 2, 3});
        Frames.write(out, new byte[0]);
        Frames.write(out, new byte[] {9});
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

        assertArrayEquals(new byte[] {1, 2, 3}, Frames.read(in, 100));
        assertArrayEquals(new byte[0], Frames.read(in, 100));
        assertArrayEquals(new byte[] {9}, Frames.read(in, 100));
        assertNull(Frames.read(in, 100)); // clean end of stream
    }

    @Test
    void rejectsAFrameOverTheLimit() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Frames.write(out, new byte[50]);

        assertThrows(ProtocolException.class,
                () -> Frames.read(new ByteArrayInputStream(out.toByteArray()), 10));
    }

    @Test
    void rejectsANegativeLength() {
        byte[] bytes = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xfe};

        assertThrows(ProtocolException.class, () -> Frames.read(new ByteArrayInputStream(bytes), 100));
    }

    @Test
    void aFrameCutOffMidwayIsAnError() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Frames.write(out, new byte[] {1, 2, 3, 4, 5});
        byte[] cut = Arrays.copyOf(out.toByteArray(), 6);

        assertThrows(EOFException.class, () -> Frames.read(new ByteArrayInputStream(cut), 100));
    }
}
