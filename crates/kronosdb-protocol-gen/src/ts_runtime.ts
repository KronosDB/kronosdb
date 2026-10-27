/** The peer sent bytes that violate the protocol. */
export class ProtocolError extends Error {}

const utf8Decoder = new TextDecoder("utf-8", { fatal: true });
const utf8Encoder = new TextEncoder();

/** A position in a byte range being read. Decoded bytes and records are views, not copies. */
export type Reader = {
  readonly buf: Uint8Array;
  readonly view: DataView;
  pos: number;
  readonly end: number;
};

export const reader = (buf: Uint8Array, start = 0, end = buf.length): Reader => ({
  buf,
  view: new DataView(buf.buffer, buf.byteOffset, buf.byteLength),
  pos: start,
  end,
});

const need = (r: Reader, n: number): void => {
  if (r.pos + n > r.end) throw new ProtocolError(`truncated: need ${n} bytes at offset ${r.pos}`);
};

export const readU8 = (r: Reader): number => {
  need(r, 1);
  return r.buf[r.pos++];
};

export const readU16 = (r: Reader): number => {
  need(r, 2);
  const v = r.view.getUint16(r.pos, true);
  r.pos += 2;
  return v;
};

export const readU32 = (r: Reader): number => {
  need(r, 4);
  const v = r.view.getUint32(r.pos, true);
  r.pos += 4;
  return v;
};

export const readU64 = (r: Reader): bigint => {
  need(r, 8);
  const v = r.view.getBigUint64(r.pos, true);
  r.pos += 8;
  return v;
};

export const readI64 = (r: Reader): bigint => {
  need(r, 8);
  const v = r.view.getBigInt64(r.pos, true);
  r.pos += 8;
  return v;
};

export const readF64 = (r: Reader): number => {
  need(r, 8);
  const v = r.view.getFloat64(r.pos, true);
  r.pos += 8;
  return v;
};

/** An i64 of milliseconds as a number: exact for any date within 285,000 years. */
export const readMillis = (r: Reader): number => {
  need(r, 8);
  const lo = r.view.getUint32(r.pos, true);
  const hi = r.view.getInt32(r.pos + 4, true);
  r.pos += 8;
  return hi * 4294967296 + lo;
};

const readFlag = (r: Reader, what: string): boolean => {
  const b = readU8(r);
  if (b > 1) throw new ProtocolError(`${what} byte ${b}`);
  return b === 1;
};

export const readBool = (r: Reader): boolean => readFlag(r, "bool");

/** An `opt<T>` presence byte. */
export const readPresent = (r: Reader): boolean => readFlag(r, "option");

const readUtf8 = (r: Reader, len: number): string => {
  need(r, len);
  const start = r.pos;
  r.pos += len;
  // Short ASCII strings (names, ids, tag keys) dominate events, and
  // TextDecoder's fixed cost is large next to them.
  if (len <= 64) {
    let s = "";
    for (let i = start; i < start + len; i++) {
      const c = r.buf[i];
      if (c >= 0x80) return utf8Decoder.decode(r.buf.subarray(start, start + len));
      s += String.fromCharCode(c);
    }
    return s;
  }
  return utf8Decoder.decode(r.buf.subarray(start, start + len));
};

export const readStr16 = (r: Reader): string => readUtf8(r, readU16(r));
export const readStr32 = (r: Reader): string => readUtf8(r, readU32(r));

const readSlice = (r: Reader, len: number): Uint8Array => {
  need(r, len);
  const out = r.buf.subarray(r.pos, r.pos + len);
  r.pos += len;
  return out;
};

export const readBytes16 = (r: Reader): Uint8Array => readSlice(r, readU16(r));
export const readBytes32 = (r: Reader): Uint8Array => readSlice(r, readU32(r));

const readList = <T>(r: Reader, n: number, item: (r: Reader) => T): T[] => {
  const out: T[] = [];
  for (let i = 0; i < n; i++) out.push(item(r));
  return out;
};

export const readList16 = <T>(r: Reader, item: (r: Reader) => T): T[] => readList(r, readU16(r), item);
export const readList32 = <T>(r: Reader, item: (r: Reader) => T): T[] => readList(r, readU32(r), item);

/** A growable buffer being written. */
export type Writer = {
  buf: Uint8Array;
  view: DataView;
  pos: number;
};

export const writer = (capacity = 1024): Writer => {
  const buf = new Uint8Array(capacity);
  return { buf, view: new DataView(buf.buffer), pos: 0 };
};

/** The bytes written so far. A view: copy it before writing more if you keep it. */
export const written = (w: Writer): Uint8Array => w.buf.subarray(0, w.pos);

export const ensure = (w: Writer, n: number): void => {
  if (w.pos + n <= w.buf.length) return;
  let size = w.buf.length * 2;
  while (size < w.pos + n) size *= 2;
  const next = new Uint8Array(size);
  next.set(w.buf.subarray(0, w.pos));
  w.buf = next;
  w.view = new DataView(next.buffer);
};

export const writeU8 = (w: Writer, v: number): void => {
  ensure(w, 1);
  w.buf[w.pos++] = v;
};

export const writeU16 = (w: Writer, v: number): void => {
  ensure(w, 2);
  w.view.setUint16(w.pos, v, true);
  w.pos += 2;
};

export const writeU32 = (w: Writer, v: number): void => {
  ensure(w, 4);
  w.view.setUint32(w.pos, v, true);
  w.pos += 4;
};

export const writeU64 = (w: Writer, v: bigint): void => {
  ensure(w, 8);
  w.view.setBigUint64(w.pos, v, true);
  w.pos += 8;
};

export const writeI64 = (w: Writer, v: bigint): void => {
  ensure(w, 8);
  w.view.setBigInt64(w.pos, v, true);
  w.pos += 8;
};

export const writeF64 = (w: Writer, v: number): void => {
  ensure(w, 8);
  w.view.setFloat64(w.pos, v, true);
  w.pos += 8;
};

export const writeMillis = (w: Writer, v: number): void => {
  if (!Number.isSafeInteger(v)) throw new ProtocolError(`millis ${v} is not a safe integer`);
  ensure(w, 8);
  const hi = Math.floor(v / 4294967296);
  w.view.setUint32(w.pos, v - hi * 4294967296, true);
  w.view.setInt32(w.pos + 4, hi, true);
  w.pos += 8;
};

export const writeLen16 = (w: Writer, n: number, ctx: string): void => {
  if (n > 0xffff) throw new ProtocolError(`${ctx}: ${n} exceeds the u16 limit`);
  writeU16(w, n);
};

export const writeLen32 = (w: Writer, n: number, ctx: string): void => {
  if (n > 0xffffffff) throw new ProtocolError(`${ctx}: ${n} exceeds the u32 limit`);
  writeU32(w, n);
};

const writeUtf8 = (w: Writer, s: string, lenBytes: 2 | 4, ctx: string): void => {
  const max = lenBytes === 2 ? 0xffff : 0xffffffff;
  ensure(w, lenBytes + s.length * 3);
  const lenAt = w.pos;
  w.pos += lenBytes;
  let n = 0;
  // ASCII fast path; fall back to encodeInto at the first wide character.
  for (; n < s.length; n++) {
    const c = s.charCodeAt(n);
    if (c >= 0x80) break;
    w.buf[w.pos + n] = c;
  }
  if (n < s.length) n += utf8Encoder.encodeInto(s.slice(n), w.buf.subarray(w.pos + n)).written;
  if (n > max) throw new ProtocolError(`${ctx}: ${n} bytes exceeds the limit`);
  if (lenBytes === 2) w.view.setUint16(lenAt, n, true);
  else w.view.setUint32(lenAt, n, true);
  w.pos += n;
};

export const writeStr16 = (w: Writer, s: string, ctx: string): void => writeUtf8(w, s, 2, ctx);
export const writeStr32 = (w: Writer, s: string, ctx: string): void => writeUtf8(w, s, 4, ctx);

const writeSlice = (w: Writer, b: Uint8Array): void => {
  ensure(w, b.length);
  w.buf.set(b, w.pos);
  w.pos += b.length;
};

export const writeBytes16 = (w: Writer, b: Uint8Array, ctx: string): void => {
  writeLen16(w, b.length, ctx);
  writeSlice(w, b);
};

export const writeBytes32 = (w: Writer, b: Uint8Array, ctx: string): void => {
  writeLen32(w, b.length, ctx);
  writeSlice(w, b);
};

/** Bytes every connection starts with, before the client's Hello. */
export const PREFACE = utf8Encoder.encode("KRONOSDB");

/** Frame header: `len u32 | kind u16 | flags u8 | stream u32`; `len` counts everything after itself. */
export const FRAME_HEADER_LEN = 11;

/** Set on the last frame a sender will send on a stream. */
export const FLAG_END = 0x01;

export type FrameHeader = {
  kind: number;
  flags: number;
  stream: number;
  /** Offset of the body within the buffer. */
  bodyStart: number;
  /** Offset just past the frame. */
  end: number;
};

/** The header of the frame at `offset`, or `undefined` if the whole frame has not arrived yet. */
export const readFrameHeader = (buf: Uint8Array, offset = 0): FrameHeader | undefined => {
  if (buf.length - offset < FRAME_HEADER_LEN) return undefined;
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const len = view.getUint32(offset, true);
  if (len < FRAME_HEADER_LEN - 4) throw new ProtocolError(`frame length ${len} is shorter than its header`);
  const end = offset + 4 + len;
  if (end > buf.length) return undefined;
  return {
    kind: view.getUint16(offset + 4, true),
    flags: buf[offset + 6],
    stream: view.getUint32(offset + 7, true),
    bodyStart: offset + FRAME_HEADER_LEN,
    end,
  };
};
