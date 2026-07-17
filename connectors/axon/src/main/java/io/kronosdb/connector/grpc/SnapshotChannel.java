package io.kronosdb.connector.grpc;

import io.kronosdb.grpc.snapshot.*;

import java.util.concurrent.CompletableFuture;

/**
 * Channel abstraction for KronosDB SnapshotStore gRPC operations.
 */
public class SnapshotChannel {

    private final SnapshotStoreGrpc.SnapshotStoreStub asyncStub;
    private final SnapshotStoreGrpc.SnapshotStoreBlockingStub blockingStub;
    private final SnapshotStoreGrpc.SnapshotStoreFutureStub futureStub;
    private final String context;

    SnapshotChannel(SnapshotStoreGrpc.SnapshotStoreStub asyncStub,
                    SnapshotStoreGrpc.SnapshotStoreBlockingStub blockingStub,
                    SnapshotStoreGrpc.SnapshotStoreFutureStub futureStub,
                    String context) {
        this.asyncStub = asyncStub;
        this.blockingStub = blockingStub;
        this.futureStub = futureStub;
        this.context = context;
    }

    /**
     * Stores a snapshot. If prune is true, older snapshots for the same key are deleted.
     */
    public CompletableFuture<AddSnapshotResponse> addSnapshot(AddSnapshotRequest request) {
        return GrpcFutures.toCompletableFuture(futureStub.add(request));
    }

    /**
     * Deletes snapshots for a key up to a given sequence.
     */
    public CompletableFuture<DeleteSnapshotsResponse> deleteSnapshots(DeleteSnapshotsRequest request) {
        return GrpcFutures.toCompletableFuture(futureStub.delete(request));
    }

    /**
     * Lists snapshots for a key within a sequence range.
     */
    public ResultStream<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest request) {
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        CollectingStreamObserver<ListSnapshotsResponse> observer = new CollectingStreamObserver<>(completionFuture);
        asyncStub.list(request, observer);
        return new ResultStream<>(observer, completionFuture);
    }

    /**
     * Gets the latest snapshot for a key.
     */
    public CompletableFuture<GetLastSnapshotResponse> getLastSnapshot(GetLastSnapshotRequest request) {
        return GrpcFutures.toCompletableFuture(futureStub.getLast(request));
    }
}
