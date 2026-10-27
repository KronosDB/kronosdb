package io.kronosdb.protocol;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A growable buffer protocol values are written to, little-endian.
 *
 * <p>Writes that cannot be represented (a string longer than its length
 * field allows, a number outside its field's range) throw
 * {@link ProtocolException} instead of truncating, and leave the writer as it
 * was before the failed write. A writer is not thread-safe.
 */
public final class Writer {

    private static final VarHandle SHORT =
            MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private byte[] buf;
    private int pos;

    public Writer() {
        this(1024);
    }

    public Writer(int capacity) {
        this.buf = new byte[Math.max(capacity, 16)];
    }

    /** The number of bytes written. */
    public int position() {
        return pos;
    }

    /** Drops everything written at or after {@code position}, to undo a partial write. */
    public void truncate(int position) {
        if (position < 0 || position > pos) {
            throw new IllegalArgumentException("cannot truncate to " + position + " at " + pos);
        }
        pos = position;
    }

    /** Forgets everything written; keeps the buffer. */
    public void reset() {
        pos = 0;
    }

    /** The internal buffer: its first {@link #position()} bytes are the content. Valid until the next write. */
    public byte[] array() {
        return buf;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buf, pos);
    }

    /** Releases a buffer grown past {@code capacity} so one huge frame does not pin memory. */
    public void shrinkTo(int capacity) {
        if (pos == 0 && buf.length > capacity) {
            buf = new byte[capacity];
        }
    }

    /** The current capacity of the internal buffer. */
    public int capacity() {
        return buf.length;
    }

    private void ensure(int n) {
        if (n > buf.length - pos) {
            long needed = (long) pos + n;
            if (needed > Integer.MAX_VALUE - 8) {
                throw new ProtocolException("output larger than 2 GiB");
            }
            long size = buf.length;
            while (size < needed) {
                size *= 2;
            }
            buf = Arrays.copyOf(buf, (int) Math.min(size, Integer.MAX_VALUE - 8));
        }
    }

    public void writeU8(int v) {
        if ((v & ~0xFF) != 0) {
            throw new ProtocolException(v + " does not fit a u8");
        }
        ensure(1);
        buf[pos++] = (byte) v;
    }

    public void writeU16(int v) {
        if ((v & ~0xFFFF) != 0) {
            throw new ProtocolException(v + " does not fit a u16");
        }
        ensure(2);
        SHORT.set(buf, pos, (short) v);
        pos += 2;
    }

    public void writeU32(long v) {
        if ((v & ~0xFFFFFFFFL) != 0) {
            throw new ProtocolException(v + " does not fit a u32");
        }
        ensure(4);
        INT.set(buf, pos, (int) v);
        pos += 4;
    }

    /** A u64 held in a {@code long} with unsigned meaning. */
    public void writeU64(long v) {
        ensure(8);
        LONG.set(buf, pos, v);
        pos += 8;
    }

    public void writeI64(long v) {
        writeU64(v);
    }

    public void writeMillis(long v) {
        writeU64(v);
    }

    public void writeF64(double v) {
        writeU64(Double.doubleToRawLongBits(v));
    }

    public void writeBool(boolean v) {
        writeU8(v ? 1 : 0);
    }

    public void writeLen16(int n, String ctx) {
        if (n > 0xFFFF) {
            throw new ProtocolException(ctx + ": " + n + " exceeds the u16 limit");
        }
        writeU16(n);
    }

    public void writeLen32(int n, String ctx) {
        writeU32(n);
    }

    /** Overwrites the u32 at {@code at}, for a length only known after its content is written. */
    public void patchU32(int at, long v) {
        if ((v & ~0xFFFFFFFFL) != 0) {
            throw new ProtocolException(v + " does not fit a u32");
        }
        INT.set(buf, at, (int) v);
    }

    public void writeStr16(String s, String ctx) {
        writeUtf8(s, 2, ctx);
    }

    public void writeStr32(String s, String ctx) {
        writeUtf8(s, 4, ctx);
    }

    private void writeUtf8(String s, int lenBytes, String ctx) {
        int chars = s.length();
        ensure(lenBytes + chars);
        int body = pos + lenBytes;
        int i = 0;
        // ASCII fast path: one byte per char, no intermediate array.
        for (; i < chars; i++) {
            char c = s.charAt(i);
            if (c >= 0x80) {
                break;
            }
            buf[body + i] = (byte) c;
        }
        int n = chars;
        if (i < chars) {
            byte[] encoded = s.getBytes(StandardCharsets.UTF_8);
            n = encoded.length;
            ensure(lenBytes + n);
            System.arraycopy(encoded, 0, buf, body, n);
        }
        if (lenBytes == 2) {
            if (n > 0xFFFF) {
                throw new ProtocolException(ctx + ": " + n + " bytes exceeds the u16 limit");
            }
            SHORT.set(buf, pos, (short) n);
        } else {
            INT.set(buf, pos, n);
        }
        pos = body + n;
    }

    public void writeBytes16(ByteBuffer b, String ctx) {
        int n = b.remaining();
        if (n > 0xFFFF) {
            throw new ProtocolException(ctx + ": " + n + " bytes exceeds the u16 limit");
        }
        writeU16(n);
        writeBuffer(b, n);
    }

    public void writeBytes32(ByteBuffer b, String ctx) {
        int n = b.remaining();
        writeU32(n);
        writeBuffer(b, n);
    }

    // Absolute get: the caller's buffer keeps its position.
    private void writeBuffer(ByteBuffer b, int n) {
        ensure(n);
        b.get(b.position(), buf, pos, n);
        pos += n;
    }

    /** Raw bytes, as the 8-byte preface. */
    public void writeRaw(byte[] bytes, int offset, int length) {
        ensure(length);
        System.arraycopy(bytes, offset, buf, pos, length);
        pos += length;
    }
}
