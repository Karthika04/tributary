package io.github.karthika04.tributary.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Small helpers for the field types the protocol uses. All numbers are big-endian
 * (Java's {@code DataOutputStream} default).
 *
 * <ul>
 *   <li>string: {@code [length: short][UTF-8 bytes]}, length -1 for null</li>
 *   <li>bytes: {@code [length: int][bytes]}, length -1 for null</li>
 * </ul>
 */
final class Wire {

    private Wire() {
    }

    static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeShort(-1);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("string too long: " + bytes.length + " bytes");
        }
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    static String readString(DataInputStream in) throws IOException {
        short length = in.readShort();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > in.available()) {
            throw new ProtocolException("invalid string length " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > in.available()) {
            throw new ProtocolException("invalid byte array length " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    static int readCount(DataInputStream in, int minBytesPerItem) throws IOException {
        int count = in.readInt();
        if (count < 0 || (long) count * minBytesPerItem > in.available()) {
            throw new ProtocolException("invalid item count " + count);
        }
        return count;
    }
}
