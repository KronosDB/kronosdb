package io.kronosdb.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * The Java half of the conformance suite: every golden frame in
 * protocol/fixtures decodes to its canonical JSON and encodes back to the
 * identical bytes. Mirrors protocol/ts/conformance.test.ts.
 */
class ConformanceTest {

    private static final Path DIR = Path.of(System.getProperty("fixtures.dir", "../fixtures")).normalize();

    private static List<String> fixtureNames() throws IOException {
        try (Stream<Path> files = Files.list(DIR)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".bin") && !n.startsWith("."))
                    .map(n -> n.substring(0, n.length() - ".bin".length()))
                    .sorted()
                    .toList();
        }
    }

    @Test
    void theFixturesArePresent() throws IOException {
        int n = fixtureNames().size();
        assertTrue(n >= 30, "only " + n + " fixtures");
    }

    @TestFactory
    Stream<DynamicTest> everyFixtureDecodesAndReencodes() throws IOException {
        return fixtureNames().stream().map(name -> DynamicTest.dynamicTest(name, () -> check(name)));
    }

    @SuppressWarnings("unchecked")
    private static void check(String name) throws IOException {
        byte[] bin = Files.readAllBytes(DIR.resolve(name + ".bin"));
        Map<String, Object> expected = (Map<String, Object>) Json.parse(Files.readString(DIR.resolve(name + ".json")));
        expected.remove("description");

        FrameHeader header = Frames.readHeader(bin, 0, bin.length);
        assertNotNull(header, "a whole frame");
        assertEquals(bin.length, header.end(), "one frame per fixture");
        Frame frame = Frames.decode(header.kind(), new Reader(bin, header.bodyStart(), header.end()));
        assertNotNull(frame, "unknown kind " + header.kind());

        Map<String, Object> actual = new LinkedHashMap<>();
        actual.put("frame", ProtocolJson.nameOf(frame));
        actual.put("stream", Long.valueOf(Integer.toUnsignedLong(header.stream())));
        actual.put("flags", Long.valueOf(header.flags()));
        actual.put("body", ProtocolJson.toJson(frame));
        assertEquals(expected, actual, () -> "decoded " + Json.stringify(actual) + "\nexpected " + Json.stringify(expected));

        Writer w = new Writer(16);
        Frames.encode(
                w,
                (int) Json.longValue(expected.get("stream")),
                Json.intValue(expected.get("flags")),
                ProtocolJson.frameFromJson((String) expected.get("frame"), expected.get("body")));
        assertArrayEquals(bin, w.toByteArray());
    }

    private static byte[] encoded(Frame frame) {
        Writer w = new Writer(16);
        Frames.encode(w, 1, 0, frame);
        return w.toByteArray();
    }

    private static Frame decoded(byte[] bin) {
        FrameHeader h = Frames.readHeader(bin, 0, bin.length);
        assertNotNull(h);
        return Frames.decode(h.kind(), new Reader(bin, h.bodyStart(), h.end()));
    }

    @Test
    void everyUnionVariantRoundTrips() {
        Writer w = new Writer();
        List<MetadataValue> values = List.of(
                new MetadataValue.OfText("ünïcode"),
                new MetadataValue.OfNumber(-42),
                new MetadataValue.OfBoolean(false),
                new MetadataValue.OfDouble(Math.PI),
                new MetadataValue.OfObject(new Payload("T", "", ByteBuffer.wrap(new byte[] {1, 2}))));
        for (MetadataValue v : values) {
            v.encode(w);
        }
        Reader r = new Reader(w.array(), 0, w.position());
        for (MetadataValue v : values) {
            assertEquals(v, MetadataValue.decode(r));
        }
        assertEquals(0, r.remaining());
        assertThrows(ProtocolException.class, () -> MetadataValue.decode(new Reader(new byte[] {9})));

        Writer events = new Writer();
        List<SubscriptionEvent> subscription = List.of(
                new SubscriptionEvent.OfComplete(new SubscriptionEnd("c", "n")),
                new SubscriptionEvent.OfCompleteExceptionally(new SubscriptionFailure(
                        "c", "n", "E1", new ErrorDetail("boom", "here", List.of("a", "b"), "E1"))));
        for (SubscriptionEvent e : subscription) {
            e.encode(events);
        }
        Reader reader = new Reader(events.array(), 0, events.position());
        for (SubscriptionEvent e : subscription) {
            assertEquals(e, SubscriptionEvent.decode(reader));
        }
    }

    @Test
    void truncatedFramesAreRejectedNotMisread() throws IOException {
        byte[] bin = Files.readAllBytes(DIR.resolve("append-conditional.bin"));
        FrameHeader header = Frames.readHeader(bin, 0, bin.length);
        assertNotNull(header);
        for (int end = header.bodyStart(); end < header.end(); end++) {
            int stop = end;
            assertThrows(
                    ProtocolException.class,
                    () -> Frames.decode(header.kind(), new Reader(bin, header.bodyStart(), stop)),
                    "decoding " + (end - header.bodyStart()) + " body bytes succeeded");
        }
        assertNull(Frames.readHeader(bin, 0, bin.length - 1));
        assertNull(Frames.readHeader(bin, 0, Frames.HEADER_LEN - 1));
    }

    @Test
    void aFrameShorterThanItsHeaderIsRejected() {
        byte[] bin = new byte[Frames.HEADER_LEN];
        bin[0] = 3; // len 3 < 7
        assertThrows(ProtocolException.class, () -> Frames.readHeader(bin, 0, bin.length));
    }

    @Test
    void bytesAfterTheDeclaredFieldsAreIgnored() {
        byte[] bin = encoded(new Head(77));
        byte[] longer = java.util.Arrays.copyOf(bin, bin.length + 5);
        FrameHeader h = Frames.readHeader(bin, 0, bin.length);
        assertNotNull(h);
        assertEquals(new Head(77), Frames.decode(h.kind(), new Reader(longer, h.bodyStart(), longer.length)));
    }

    @Test
    void anUnknownKindDecodesToNull() {
        assertNull(Frames.decode(0x7fff, new Reader(new byte[0])));
    }

    @Test
    void enumsKeepValuesThisVersionDoesNotKnow() {
        Hello hello = new Hello(1, 0, 3, "c", "id", CredentialKind.of(9), "");
        byte[] bin = encoded(hello);
        Hello back = (Hello) decoded(bin);
        assertEquals(9, back.credentialKind().value());
        assertNull(back.credentialKind().variantName());
        assertEquals(CredentialKind.of(9), back.credentialKind());
        assertSame(CredentialKind.TOKEN, CredentialKind.of(1));
        assertArrayEquals(bin, encoded(back));

        RequestFailed failed = (RequestFailed) decoded(encoded(new RequestFailed(ErrorCode.of(999), "?")));
        assertEquals(999, failed.code().value());
    }

    @Test
    void unsignedValuesKeepTheirBits() {
        Appended a = new Appended(-1L, 0xFFFFFFFFL, Long.MIN_VALUE);
        Appended back = (Appended) decoded(encoded(a));
        assertEquals(a, back);
        assertEquals("18446744073709551615", Long.toUnsignedString(back.firstPosition()));
        assertEquals(4294967295L, back.count());
        assertEquals("18446744073709551615", ProtocolJson.toJson(new Appended(-1L, 1, 0)) instanceof Map<?, ?> m
                ? m.get("first_position")
                : null);
    }

    @Test
    void valuesOutsideTheirFieldAreRefused() {
        Writer w = new Writer();
        assertThrows(ProtocolException.class, () -> new ContextBound(70_000).encode(w));
        assertThrows(ProtocolException.class, () -> new Credit(-1).encode(w));
        assertThrows(ProtocolException.class, () -> new Credit(1L << 32).encode(w));
        assertEquals(0, w.position());
    }

    @Test
    void stringsThatDoNotFitTheirLengthFieldFailToEncodeAndLeaveTheWriterAlone() {
        Writer w = new Writer();
        w.writeU8(7);
        ProtocolException e = assertThrows(
                ProtocolException.class,
                () -> Frames.encode(w, 0, 0, new BindContext(1, "x".repeat(70_000))));
        assertTrue(e.getMessage().contains("BindContext.name"), e.getMessage());
        assertEquals(1, w.position(), "nothing of the failed frame stays in the writer");
        assertEquals(7, w.array()[0]);
    }

    @Test
    void stringsRoundTripAsUtf8() {
        String text = "ascii, ünïcødé ✓, and an emoji 🗄️";
        BindContext back = (BindContext) decoded(encoded(new BindContext(2, text)));
        assertEquals(text, back.name());
        byte[] bin = encoded(new BindContext(2, text));
        // len u32 | kind u16 | flags u8 | stream u32 | id u16 | name len u16 | name
        int nameLen = Reader.u16At(bin, Frames.HEADER_LEN + 2);
        assertEquals(text.getBytes(StandardCharsets.UTF_8).length, nameLen);
    }

    @Test
    void invalidUtf8IsRejected() {
        byte[] bin = encoded(new BindContext(1, "abc"));
        bin[bin.length - 1] = (byte) 0xFF;
        assertThrows(ProtocolException.class, () -> decoded(bin));
    }

    @Test
    void boolAndOptionBytesOtherThanZeroOrOneAreRejected() {
        byte[] bin = encoded(new ClientAck(new Ack("i", true, null)));
        // header | instruction_id str16 (2 + 1) | success | error presence
        int success = Frames.HEADER_LEN + 3;
        int present = success + 1;
        assertEquals(1, bin[success]);
        assertEquals(0, bin[present]);

        byte[] badBool = bin.clone();
        badBool[success] = 2;
        assertThrows(ProtocolException.class, () -> decoded(badBool));

        byte[] badOption = bin.clone();
        badOption[present] = 2;
        assertThrows(ProtocolException.class, () -> decoded(badOption));
    }

    @Test
    void decodedBytesAreSlicesOfTheFrameNotCopies() throws IOException {
        byte[] bin = Files.readAllBytes(DIR.resolve("events-batch.bin"));
        Events events = (Events) decoded(bin);
        ByteBuffer payload = events.events().get(0).event().payload();
        assertSame(bin, payload.array());
        assertTrue(payload.arrayOffset() > 0);
        assertEquals(0, payload.position());
    }

    @Test
    void aRecordThatClaimsMoreThanTheFrameHasIsRejected() throws IOException {
        byte[] bin = Files.readAllBytes(DIR.resolve("events-batch.bin"));
        FrameHeader h = Frames.readHeader(bin, 0, bin.length);
        assertNotNull(h);
        // The first record's length sits after the record count (u32).
        int lenAt = h.bodyStart() + 4;
        byte[] bad = bin.clone();
        bad[lenAt + 3] = 0x7f;
        assertThrows(ProtocolException.class, () -> decoded(bad));

        byte[] zero = bin.clone();
        java.util.Arrays.fill(zero, lenAt, lenAt + 4, (byte) 0);
        assertThrows(ProtocolException.class, () -> decoded(zero));
    }
}
