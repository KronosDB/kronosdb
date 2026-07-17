package io.kronosdb.connector.axon.snapshot;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.kronosdb.connector.grpc.KronosDbConnection;
import io.kronosdb.grpc.snapshot.AddSnapshotRequest;
import io.kronosdb.grpc.snapshot.GetLastSnapshotRequest;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A KronosDB-backed implementation of {@link SnapshotStore}.
 * <p>
 * Stores and retrieves aggregate snapshots via the KronosDB Snapshot Store gRPC service.
 * Snapshots are keyed by qualified name + identifier, matching the Axon Framework convention.
 */
public class KronosDbSnapshotStore implements SnapshotStore {

    private static final String POSITION_TYPE_KEY = "__AxonFramework__:Position-Type";
    private static final ByteString NUL = ByteString.copyFrom(new byte[]{0});

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

        ByteString key = makeKey(qualifiedName, identifier);
        ByteString data = converter.convert(snapshot.payload(), byte[].class) instanceof byte[] ba
                ? ByteString.copyFrom(ba) : ByteString.EMPTY;

        long sequence = switch (snapshot.position()) {
            case GlobalIndexPosition gip -> GlobalIndexPosition.toIndex(snapshot.position());
            case AggregateSequenceNumberPosition asnp ->
                    AggregateSequenceNumberPosition.toSequenceNumber(snapshot.position());
            default -> throw new IllegalArgumentException("Unsupported position type: " + snapshot.position());
        };

        String positionType = switch (snapshot.position()) {
            case GlobalIndexPosition gip -> "GIP";
            case AggregateSequenceNumberPosition asnp -> "ASNP";
            default -> throw new IllegalArgumentException("Unsupported position type: " + snapshot.position());
        };

        return connection.snapshotChannel()
                .addSnapshot(AddSnapshotRequest.newBuilder()
                        .setKey(key)
                        .setSequence(sequence)
                        .setPrune(true)
                        .setSnapshot(io.kronosdb.grpc.snapshot.Snapshot.newBuilder()
                                .setName(qualifiedName.fullName())
                                .setVersion(snapshot.version())
                                .setTimestamp(snapshot.timestamp().toEpochMilli())
                                .setPayload(data)
                                .putAllMetadata(snapshot.metadata())
                                .putMetadata(POSITION_TYPE_KEY, positionType))
                        .build())
                .thenApply(v -> null);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier) {
        Objects.requireNonNull(qualifiedName);
        Objects.requireNonNull(identifier);

        ByteString key = makeKey(qualifiedName, identifier);

        return connection.snapshotChannel()
                .getLastSnapshot(GetLastSnapshotRequest.newBuilder().setKey(key).build())
                .thenApply(sr -> {
                    io.kronosdb.grpc.snapshot.Snapshot snapshot = sr.getSnapshot();
                    if (snapshot == null) {
                        return null;
                    }

                    Map<String, String> metadata = new HashMap<>(snapshot.getMetadataMap());
                    String positionType = metadata.remove(POSITION_TYPE_KEY);
                    Position position = switch (positionType) {
                        case "GIP" -> new GlobalIndexPosition(sr.getSequence());
                        case "ASNP" -> new AggregateSequenceNumberPosition(sr.getSequence());
                        case null, default ->
                                throw new IllegalArgumentException("Unexpected position type: " + positionType);
                    };

                    return new Snapshot(
                            position,
                            snapshot.getVersion(),
                            snapshot.getPayload().toByteArray(),
                            Instant.ofEpochMilli(snapshot.getTimestamp()),
                            metadata
                    );
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
}
