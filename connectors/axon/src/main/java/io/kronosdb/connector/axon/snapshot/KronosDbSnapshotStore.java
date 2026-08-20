package io.kronosdb.connector.axon.snapshot;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kronosdb.connector.grpc.KronosDbConnection;
import io.kronosdb.grpc.eventstore.AppendSnapshotRequest;
import io.kronosdb.grpc.eventstore.GetSnapshotRequest;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A KronosDB-backed implementation of {@link SnapshotStore}.
 * <p>
 * Stores and retrieves aggregate snapshots via the KronosDB EventStore snapshot
 * RPCs (ADR-0005: snapshots ride the replicated event log). KronosDB stores one
 * opaque state blob per key, so this adapter encodes the Axon snapshot's
 * version, timestamp, metadata, position type, and payload into the blob
 * itself; the numeric position value travels in the wire {@code position}
 * field. Snapshots are keyed by qualified name + identifier, matching the
 * Axon Framework convention. Superseded snapshots need no pruning — a newer
 * record for the same key wins by log order.
 */
public class KronosDbSnapshotStore implements SnapshotStore {

    private static final ByteString NUL = ByteString.copyFrom(new byte[]{0});

    /** Format version of the encoded state blob. */
    private static final byte STATE_FORMAT_V1 = 1;
    private static final byte POSITION_GLOBAL_INDEX = 0;
    private static final byte POSITION_AGGREGATE_SEQUENCE = 1;

    private final KronosDbConnection connection;
    private final Converter converter;

    public KronosDbSnapshotStore(KronosDbConnection connection, Converter converter) {
        this.connection = Objects.requireNonNull(connection);
        this.converter = Objects.requireNonNull(converter);
    }

    private ByteString makeKey(QualifiedName qn, Object identifier) {
        return ByteString.copyFrom(converter.convert(qn.name(), byte[].class))
                .concat(NUL)
                .concat(ByteString.copyFrom(converter.convert(identifier, byte[].class)));
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot) {
        Objects.requireNonNull(qualifiedName);
        Objects.requireNonNull(identifier);
        Objects.requireNonNull(snapshot);

        long position = switch (snapshot.position()) {
            case GlobalIndexPosition gip -> GlobalIndexPosition.toIndex(snapshot.position());
            case AggregateSequenceNumberPosition asnp ->
                    AggregateSequenceNumberPosition.toSequenceNumber(snapshot.position());
            default -> throw new IllegalArgumentException("Unsupported position type: " + snapshot.position());
        };

        byte positionType = switch (snapshot.position()) {
            case GlobalIndexPosition gip -> POSITION_GLOBAL_INDEX;
            case AggregateSequenceNumberPosition asnp -> POSITION_AGGREGATE_SEQUENCE;
            default -> throw new IllegalArgumentException("Unsupported position type: " + snapshot.position());
        };

        byte[] payload = converter.convert(snapshot.payload(), byte[].class) instanceof byte[] ba
                ? ba : new byte[0];

        return connection.eventStoreChannel()
                .appendSnapshot(AppendSnapshotRequest.newBuilder()
                        .setKey(makeKey(qualifiedName, identifier))
                        .setState(encodeState(snapshot, positionType, payload))
                        .setPosition(position)
                        .build())
                .thenApply(v -> null);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier) {
        Objects.requireNonNull(qualifiedName);
        Objects.requireNonNull(identifier);

        return connection.eventStoreChannel()
                .getSnapshot(GetSnapshotRequest.newBuilder()
                        .setKey(makeKey(qualifiedName, identifier))
                        .build())
                .thenApply(sr -> {
                    if (!sr.hasSnapshot()) {
                        return null;
                    }
                    return decodeState(sr.getSnapshot().getState(), sr.getSnapshot().getPosition());
                })
                .exceptionally(e -> {
                    while (e instanceof CompletionException) {
                        e = e.getCause();
                    }
                    if (e instanceof StatusRuntimeException sre
                            && (sre.getStatus().getCode() == Status.Code.CANCELLED
                            || sre.getStatus().getCode() == Status.Code.NOT_FOUND)) {
                        return null;
                    }
                    throw new CompletionException(
                            "Snapshot loading failed for %s with identifier %s"
                                    .formatted(qualifiedName.toString(), identifier.toString()), e);
                });
    }

    static ByteString encodeState(Snapshot snapshot, byte positionType, byte[] payload) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(payload.length + 128);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(STATE_FORMAT_V1);
            out.writeByte(positionType);
            out.writeUTF(snapshot.version());
            out.writeLong(snapshot.timestamp().toEpochMilli());
            out.writeInt(snapshot.metadata().size());
            for (Map.Entry<String, String> entry : snapshot.metadata().entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            out.writeInt(payload.length);
            out.write(payload);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode snapshot state", e);
        }
        return ByteString.copyFrom(bytes.toByteArray());
    }

    static Snapshot decodeState(ByteString state, long positionValue) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(state.toByteArray()))) {
            byte formatVersion = in.readByte();
            if (formatVersion != STATE_FORMAT_V1) {
                throw new IllegalArgumentException("Unsupported snapshot state format: " + formatVersion);
            }
            byte positionType = in.readByte();
            Position position = switch (positionType) {
                case POSITION_GLOBAL_INDEX -> new GlobalIndexPosition(positionValue);
                case POSITION_AGGREGATE_SEQUENCE -> new AggregateSequenceNumberPosition(positionValue);
                default -> throw new IllegalArgumentException("Unexpected position type: " + positionType);
            };
            String version = in.readUTF();
            Instant timestamp = Instant.ofEpochMilli(in.readLong());
            int metadataCount = in.readInt();
            Map<String, String> metadata = new HashMap<>(metadataCount);
            for (int i = 0; i < metadataCount; i++) {
                metadata.put(in.readUTF(), in.readUTF());
            }
            byte[] payload = new byte[in.readInt()];
            in.readFully(payload);
            return new Snapshot(position, version, payload, timestamp, metadata);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decode snapshot state", e);
        }
    }
}
