// The TypeScript half of the conformance suite: every golden frame decodes to
// its canonical JSON and encodes back to the identical bytes.
//
//   node --experimental-strip-types --test protocol/ts/conformance.test.ts

import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import * as P from "./kronosdb-protocol.ts";
import { frameBodyToJson, frameFromJson } from "./kronosdb-protocol-json.ts";

const dir = join(dirname(fileURLToPath(import.meta.url)), "..", "fixtures");
const names = readdirSync(dir)
  .filter((f) => f.endsWith(".bin") && !f.startsWith("."))
  .map((f) => f.slice(0, -".bin".length))
  .sort();

const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");

test("the fixtures are present", () => {
  assert.ok(names.length >= 30, `only ${names.length} fixtures`);
});

for (const name of names) {
  test(name, () => {
    const bin = new Uint8Array(readFileSync(join(dir, `${name}.bin`)));
    const expected = JSON.parse(readFileSync(join(dir, `${name}.json`), "utf8"));
    delete expected.description;

    const header = P.readFrameHeader(bin);
    assert.ok(header, "a whole frame");
    assert.equal(header.end, bin.length, "one frame per fixture");
    const frame = P.decodeFrameBody(header.kind, P.reader(bin, header.bodyStart, header.end));
    assert.ok(frame, `unknown kind ${header.kind}`);
    assert.deepEqual(
      { frame: frame.name, stream: header.stream, flags: header.flags, body: frameBodyToJson(frame) },
      expected,
    );

    const w = P.writer(16);
    P.encodeFrame(w, expected.stream, expected.flags, frameFromJson(expected.frame, expected.body));
    assert.equal(hex(P.written(w)), hex(bin));
  });
}

test("every union variant round-trips", () => {
  const w = P.writer();
  const values: P.MetadataValue[] = [
    { kind: "text", value: "ünïcode" },
    { kind: "number", value: -42n },
    { kind: "boolean", value: false },
    { kind: "double", value: Math.PI },
    { kind: "object", value: { typeName: "T", revision: "", data: new Uint8Array([1, 2]) } },
  ];
  for (const v of values) P.encodeMetadataValue(w, v);
  const r = P.reader(P.written(w));
  for (const v of values) assert.deepEqual(P.decodeMetadataValue(r), v);
  assert.equal(r.pos, w.pos);
  assert.throws(() => P.decodeMetadataValue(P.reader(new Uint8Array([9]))), P.ProtocolError);
});

test("truncated frames are rejected, not misread", () => {
  const bin = new Uint8Array(readFileSync(join(dir, "append-conditional.bin")));
  const header = P.readFrameHeader(bin)!;
  for (let end = header.bodyStart; end < header.end; end++) {
    assert.throws(
      () => P.decodeFrameBody(header.kind, P.reader(bin, header.bodyStart, end)),
      P.ProtocolError,
      `decoding ${end - header.bodyStart} body bytes succeeded`,
    );
  }
  assert.equal(P.readFrameHeader(bin.subarray(0, bin.length - 1)), undefined);
});

test("strings that do not fit their length field fail to encode", () => {
  const w = P.writer();
  assert.throws(
    () => P.encodeFrame(w, 0, 0, { name: "BindContext", body: { id: 1, name: "x".repeat(70_000) } }),
    /BindContext\.name/,
  );
});
