package io.github.karthika04.tributary.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Records travel in blocks so a whole block can be compressed at once (compressing
 * many records together shrinks them far more than compressing each alone).
 *
 * <pre>
 * [compression: byte][uncompressedSize: int][payloadSize: int][payload]
 * payload (after decompressing) = [count: int] then per record:
 *     produce: [key: bytes][value: bytes]
 *     fetch:   [offset: long][timestamp: long][key: bytes][value: bytes]
 * </pre>
 */
final class RecordBlocks {

    private static final int MIN_PRODUCED_BYTES = 4 + 4;
    private static final int MIN_FETCHED_BYTES = 8 + 8 + 4 + 4;

    /** Largest block a receiver will decompress: four times the default frame limit. */
    static final int MAX_UNCOMPRESSED_BLOCK_BYTES = 64 * 1024 * 1024;

    private RecordBlocks() {
    }

    static void writeProduced(DataOutputStream out, Compression compression, List<ProducedRecord> records)
            throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(raw);
        body.writeInt(records.size());
        for (ProducedRecord r : records) {
            Wire.writeBytes(body, r.key());
            Wire.writeBytes(body, r.value());
        }
        writeBlock(out, compression, raw.toByteArray());
    }

    static void writeFetched(DataOutputStream out, Compression compression, List<FetchedRecord> records)
            throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(raw);
        body.writeInt(records.size());
        for (FetchedRecord r : records) {
            body.writeLong(r.offset());
            body.writeLong(r.timestamp());
            Wire.writeBytes(body, r.key());
            Wire.writeBytes(body, r.value());
        }
        writeBlock(out, compression, raw.toByteArray());
    }

    record ProducedBlock(Compression compression, List<ProducedRecord> records) {
    }

    static ProducedBlock readProduced(DataInputStream in) throws IOException {
        Block block = readBlock(in);
        DataInputStream body = block.body();
        int count = Wire.readCount(body, MIN_PRODUCED_BYTES);
        List<ProducedRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] key = Wire.readBytes(body);
            byte[] value = Wire.readBytes(body);
            if (value == null) {
                throw new ProtocolException("record value must not be null");
            }
            records.add(new ProducedRecord(key, value));
        }
        expectEnd(body);
        return new ProducedBlock(block.compression(), records);
    }

    static List<FetchedRecord> readFetched(DataInputStream in) throws IOException {
        DataInputStream body = readBlock(in).body();
        int count = Wire.readCount(body, MIN_FETCHED_BYTES);
        List<FetchedRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long offset = body.readLong();
            long timestamp = body.readLong();
            byte[] key = Wire.readBytes(body);
            byte[] value = Wire.readBytes(body);
            if (value == null) {
                throw new ProtocolException("record value must not be null");
            }
            records.add(new FetchedRecord(offset, timestamp, key, value));
        }
        expectEnd(body);
        return records;
    }

    private static void writeBlock(DataOutputStream out, Compression compression, byte[] raw) throws IOException {
        byte[] payload = switch (compression) {
            case NONE -> raw;
            case GZIP -> gzip(raw);
        };
        out.writeByte(compression.code());
        out.writeInt(raw.length);
        out.writeInt(payload.length);
        out.write(payload);
    }

    private record Block(Compression compression, DataInputStream body) {
    }

    private static Block readBlock(DataInputStream in) throws IOException {
        Compression compression = Compression.fromCode(in.readByte());
        int rawSize = in.readInt();
        int payloadSize = in.readInt();
        if (payloadSize < 0 || payloadSize > in.available() || rawSize < 0) {
            throw new ProtocolException("invalid record block sizes: raw " + rawSize + ", payload " + payloadSize);
        }
        byte[] payload = new byte[payloadSize];
        in.readFully(payload);
        byte[] raw = switch (compression) {
            case NONE -> {
                if (rawSize != payloadSize) {
                    throw new ProtocolException("uncompressed block size mismatch");
                }
                yield payload;
            }
            case GZIP -> gunzip(payload, rawSize);
        };
        return new Block(compression, new DataInputStream(new ByteArrayInputStream(raw)));
    }

    private static byte[] gzip(byte[] raw) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream(Math.max(64, raw.length / 4));
        try (GZIPOutputStream gz = new GZIPOutputStream(compressed)) {
            gz.write(raw);
        }
        return compressed.toByteArray();
    }

    /**
     * Decompresses, refusing to produce more than the declared size. Without this
     * limit a tiny malicious payload (a "zip bomb") could expand to gigabytes.
     *
     * <p>The output grows as data actually comes out of the decompressor, rather than
     * being allocated at the declared size up front: an earlier version did that, and
     * CodecFuzzTest found that a 150-byte message declaring 2 GB made the receiver
     * allocate 2 GB, which is the very attack this method exists to stop.
     */
    private static byte[] gunzip(byte[] payload, int expectedSize) throws IOException {
        if (expectedSize > MAX_UNCOMPRESSED_BLOCK_BYTES) {
            throw new ProtocolException("compressed block declares " + expectedSize
                    + " bytes uncompressed; the limit is " + MAX_UNCOMPRESSED_BLOCK_BYTES);
        }
        ByteArrayOutputStream raw = new ByteArrayOutputStream(Math.min(expectedSize, Math.max(256, payload.length * 4)));
        byte[] chunk = new byte[8192];
        try (InputStream gz = new GZIPInputStream(new ByteArrayInputStream(payload))) {
            int n;
            while ((n = gz.read(chunk)) != -1) {
                if (raw.size() + n > expectedSize) {
                    throw new ProtocolException("compressed block is longer than declared");
                }
                raw.write(chunk, 0, n);
            }
        } catch (java.util.zip.ZipException | java.io.EOFException e) {
            throw new ProtocolException("corrupt compressed block: " + e.getMessage(), e);
        }
        if (raw.size() != expectedSize) {
            throw new ProtocolException("compressed block is shorter than declared");
        }
        return raw.toByteArray();
    }

    private static void expectEnd(DataInputStream body) throws IOException {
        if (body.available() != 0) {
            throw new ProtocolException(body.available() + " unexpected bytes after the last record");
        }
    }
}
