package io.github.karthika04.tributary.protocol;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * TCP delivers a stream of bytes, not separate messages. To know where one message
 * ends and the next begins, every message is sent as a frame:
 * {@code [length: 4 bytes][that many bytes of message]}.
 */
public final class Frames {

    /** Largest frame accepted by default: 16 MiB. */
    public static final int DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;

    private Frames() {
    }

    /** Writes one frame. The caller decides when to flush. */
    public static void write(OutputStream out, byte[] payload) throws IOException {
        out.write(new byte[] {
                (byte) (payload.length >>> 24), (byte) (payload.length >>> 16),
                (byte) (payload.length >>> 8), (byte) payload.length});
        out.write(payload);
    }

    /**
     * Reads one frame.
     *
     * @return the frame's bytes, or null if the other side closed the connection
     *         cleanly between frames
     * @throws ProtocolException if the length is negative or above {@code maxFrameBytes},
     *                           which protects the reader from allocating huge buffers
     * @throws EOFException if the connection closed in the middle of a frame
     */
    public static byte[] read(InputStream in, int maxFrameBytes) throws IOException {
        int b1 = in.read();
        if (b1 < 0) {
            return null;
        }
        DataInputStream data = new DataInputStream(in);
        int length = (b1 << 24) | (data.readUnsignedByte() << 16) | (data.readUnsignedByte() << 8)
                | data.readUnsignedByte();
        if (length < 0 || length > maxFrameBytes) {
            throw new ProtocolException("frame of " + length + " bytes exceeds the limit of " + maxFrameBytes);
        }
        byte[] payload = new byte[length];
        data.readFully(payload);
        return payload;
    }
}
