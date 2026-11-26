package io.github.karthika04.tributary.storage;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * The ".log" file of a segment: record frames written one after another, never
 * changed in place. New data is only ever added at the end.
 *
 * <p>Why append-only? Writing to the end of a file is the fastest thing a disk can do,
 * and data that never changes is easy to cache, copy and recover after a crash.
 *
 * <p>Threading: only one thread appends at a time (the {@link Log} holds its write
 * lock). Any number of threads may read at once, because {@link FileChannel}
 * positional reads do not share a file pointer.
 */
final class Store implements Closeable {

    private final Path path;
    private final FileChannel channel;
    /** Bytes written so far. volatile so readers on other threads see the latest value. */
    private volatile long size;

    private Store(Path path, FileChannel channel, long size) {
        this.path = path;
        this.channel = channel;
        this.size = size;
    }

    static Store open(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new Store(path, channel, channel.size());
    }

    /**
     * Writes all remaining bytes of {@code frames} at the end of the file.
     * A buffer may hold many frames, so a whole batch costs a single write call.
     *
     * @return the file position where the first byte was written
     */
    long append(ByteBuffer frames) throws IOException {
        long start = size;
        long position = start;
        while (frames.hasRemaining()) {
            position += channel.write(frames, position);
        }
        size = position;
        return start;
    }

    /**
     * Reads the frame that starts at {@code position}.
     *
     * @param maxFrameSize frames bigger than this are treated as corrupt, so a damaged
     *                     length field cannot make us allocate gigabytes
     * @throws CorruptRecordException if the frame is cut off or its length is impossible
     */
    ByteBuffer readFrame(long position, int maxFrameSize) throws IOException {
        long fileSize = size;
        if (position < 0 || position + RecordCodec.LENGTH_FIELD_SIZE > fileSize) {
            throw new CorruptRecordException("no frame header at position " + position
                    + " (file size " + fileSize + ")");
        }
        ByteBuffer lengthBuffer = ByteBuffer.allocate(RecordCodec.LENGTH_FIELD_SIZE);
        readFully(lengthBuffer, position);
        int length = lengthBuffer.getInt(0);
        int frameSize = RecordCodec.LENGTH_FIELD_SIZE + length;
        if (length <= 0 || frameSize > maxFrameSize || frameSize < RecordCodec.FRAME_OVERHEAD) {
            throw new CorruptRecordException("impossible frame length " + length + " at position " + position);
        }
        if (position + frameSize > fileSize) {
            throw new CorruptRecordException("frame at position " + position + " is cut off: needs "
                    + frameSize + " bytes, only " + (fileSize - position) + " on disk");
        }
        ByteBuffer frame = ByteBuffer.allocate(frameSize);
        readFully(frame, position);
        return frame.flip();
    }

    private void readFully(ByteBuffer dst, long position) throws IOException {
        long p = position;
        while (dst.hasRemaining()) {
            int n = channel.read(dst, p);
            if (n < 0) {
                throw new EOFException("unexpected end of " + path + " at position " + p);
            }
            p += n;
        }
    }

    long size() {
        return size;
    }

    Path path() {
        return path;
    }

    /** Cuts the file back to {@code newSize} bytes. Used to drop a half-written tail after a crash. */
    void truncateTo(long newSize) throws IOException {
        channel.truncate(newSize);
        size = newSize;
    }

    /**
     * Forces written bytes from the operating system's cache onto the physical disk.
     * Until this runs, a power cut could lose recent writes even though write() returned.
     */
    void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            channel.force(true);
            channel.close();
        }
    }
}
