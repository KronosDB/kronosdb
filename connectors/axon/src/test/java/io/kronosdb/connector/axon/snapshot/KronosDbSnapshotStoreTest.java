package io.kronosdb.connector.axon.snapshot;

import com.google.protobuf.ByteString;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Roundtrip proof of the state-blob codec: KronosDB stores one opaque blob,
 * so everything Axon needs back (version, timestamp, metadata, position
 * type, payload) must survive encode → decode byte-exact, with the numeric
 * position value traveling separately in the wire position field.
 */
class KronosDbSnapshotStoreTest {

    private static final byte GIP = 0;
    private static final byte ASNP = 1;

    private static Snapshot snapshot(org.axonframework.eventsourcing.eventstore.Position position,
                                     byte[] payload,
                                     Map<String, String> metadata) {
        return new Snapshot(position, "2.1", payload, Instant.ofEpochMilli(1755640000000L), metadata);
    }

    @Test
    void globalIndexPositionRoundtrips() {
        byte[] payload = {0x00, 0x01, (byte) 0xFF, 0x42};
        Snapshot original = snapshot(new GlobalIndexPosition(42), payload,
                Map.of("source", "order-service", "trace", "abc-123"));

        ByteString state = KronosDbSnapshotStore.encodeState(original, GIP, payload);
        Snapshot decoded = KronosDbSnapshotStore.decodeState(state, 42);

        assertInstanceOf(GlobalIndexPosition.class, decoded.position());
        assertEquals(42, GlobalIndexPosition.toIndex(decoded.position()));
        assertEquals("2.1", decoded.version());
        assertEquals(Instant.ofEpochMilli(1755640000000L), decoded.timestamp());
        assertEquals(original.metadata(), decoded.metadata());
        assertArrayEquals(payload, (byte[]) decoded.payload());
    }

    @Test
    void aggregateSequenceNumberPositionRoundtrips() {
        byte[] payload = new byte[0];
        Snapshot original = snapshot(new AggregateSequenceNumberPosition(7), payload, Map.of());

        ByteString state = KronosDbSnapshotStore.encodeState(original, ASNP, payload);
        Snapshot decoded = KronosDbSnapshotStore.decodeState(state, 7);

        assertInstanceOf(AggregateSequenceNumberPosition.class, decoded.position());
        assertEquals(7, AggregateSequenceNumberPosition.toSequenceNumber(decoded.position()));
        assertArrayEquals(payload, (byte[]) decoded.payload());
        assertEquals(Map.of(), decoded.metadata());
    }

    @Test
    void unicodeMetadataSurvives() {
        byte[] payload = {1};
        Snapshot original = snapshot(new GlobalIndexPosition(1), payload,
                Map.of("kund", "Åsa Öberg", "emoji", "🗄️"));

        Snapshot decoded = KronosDbSnapshotStore.decodeState(
                KronosDbSnapshotStore.encodeState(original, GIP, payload), 1);

        assertEquals(original.metadata(), decoded.metadata());
    }

    @Test
    void unknownFormatVersionIsRejectedNotMisread() {
        byte[] payload = {1};
        byte[] encoded = KronosDbSnapshotStore
                .encodeState(snapshot(new GlobalIndexPosition(1), payload, Map.of()), GIP, payload)
                .toByteArray();
        encoded[0] = 99; // future format version

        assertThrows(IllegalArgumentException.class,
                () -> KronosDbSnapshotStore.decodeState(ByteString.copyFrom(encoded), 1));
    }

    @Test
    void unknownPositionTypeIsRejected() {
        byte[] payload = {1};
        byte[] encoded = KronosDbSnapshotStore
                .encodeState(snapshot(new GlobalIndexPosition(1), payload, Map.of()), GIP, payload)
                .toByteArray();
        encoded[1] = 9; // not GIP/ASNP

        assertThrows(IllegalArgumentException.class,
                () -> KronosDbSnapshotStore.decodeState(ByteString.copyFrom(encoded), 1));
    }
}
