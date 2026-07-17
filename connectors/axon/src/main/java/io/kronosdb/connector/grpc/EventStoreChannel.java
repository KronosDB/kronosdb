package io.kronosdb.connector.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import io.kronosdb.grpc.eventstore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Channel abstraction for KronosDB EventStore gRPC operations.
 * Wraps the raw gRPC stubs with a clean async API.
 */
public class EventStoreChannel {

    private static final Logger logger = LoggerFactory.getLogger(EventStoreChannel.class);

    private final EventStoreGrpc.EventStoreStub asyncStub;
    private final EventStoreGrpc.EventStoreBlockingStub blockingStub;
    private final EventStoreGrpc.EventStoreFutureStub futureStub;
    private final String context;

    EventStoreChannel(EventStoreGrpc.EventStoreStub asyncStub,
                      EventStoreGrpc.EventStoreBlockingStub blockingStub,
                      EventStoreGrpc.EventStoreFutureStub futureStub,
                      String context) {
        this.asyncStub = asyncStub;
        this.blockingStub = blockingStub;
        this.futureStub = futureStub;
        this.context = context;
    }

    /**
     * Starts an append transaction. Events are batched and sent as a stream.
     * Call {@link AppendTransaction#commit()} to finalize.
     */
    public AppendTransaction startAppendTransaction() {
        return new AppendTransaction(asyncStub);
    }

    /**
     * Sources events matching the given criteria (finite stream).
     *
     * @param request the source request with criteria and starting position
     * @return an iterator of source responses
     */
    public ResultStream<SourceResponse> source(SourceRequest request) {
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        CollectingStreamObserver<SourceResponse> observer = new CollectingStreamObserver<>(completionFuture);
        asyncStub.source(request, observer);
        return new ResultStream<>(observer, completionFuture);
    }

    /** Default number of permits to grant initially and on each refill. */
    private static final long DEFAULT_PERMITS = 500;

    /** When permits drop to this fraction of the batch, grant more. */
    private static final double REFILL_THRESHOLD = 0.25;

    /**
     * Streams events matching the given criteria (infinite live-tailing stream)
     * with permit-based flow control.
     *
     * <p>Opens a bidirectional stream: sends a {@link StreamSubscribe} as the first
     * message with initial permits, then automatically grants more permits as
     * events are consumed to keep the pipeline full.</p>
     *
     * @param fromSequence  inclusive starting sequence position
     * @param criteria      criteria to filter events (may be empty for all events)
     * @return a result stream of streaming responses
     */
    public ResultStream<StreamResponse> stream(long fromSequence, List<io.kronosdb.grpc.eventstore.Criterion> criteria) {
        return stream(fromSequence, criteria, DEFAULT_PERMITS);
    }

    /**
     * Streams events with explicit permit batch size.
     */
    public ResultStream<StreamResponse> stream(long fromSequence,
                                                List<io.kronosdb.grpc.eventstore.Criterion> criteria,
                                                long permitBatchSize) {
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        CollectingStreamObserver<StreamResponse> observer = new CollectingStreamObserver<>(completionFuture);

        // Open the bidirectional stream.
        StreamObserver<StreamControl> requestStream = asyncStub.stream(observer);

        // Send the subscribe message with initial permits. batch_size 0 lets
        // the server pick its default; it never sends more unconsumed events
        // than granted permits, so the effective cap is the permit budget.
        StreamSubscribe subscribe = StreamSubscribe.newBuilder()
                .setFromSequence(fromSequence)
                .addAllCriteria(criteria)
                .setInitialPermits(permitBatchSize)
                .setBatchSize(0)
                .build();
        requestStream.onNext(StreamControl.newBuilder().setSubscribe(subscribe).build());

        // Permits count events: a batch costs its event count, heartbeats cost 0.
        return new FlowControlledResultStream<>(observer, completionFuture, requestStream, permitBatchSize,
                response -> response.hasBatch() ? response.getBatch().getEventsCount() : 0);
    }

    /**
     * Gets the current head (next position to be assigned) of the event store.
     */
    public CompletableFuture<GetHeadResponse> head() {
        return GrpcFutures.toCompletableFuture(futureStub.getHead(GetHeadRequest.getDefaultInstance()));
    }

    /**
     * Gets the current tail (first event position) of the event store.
     */
    public CompletableFuture<GetTailResponse> tail() {
        return GrpcFutures.toCompletableFuture(futureStub.getTail(GetTailRequest.getDefaultInstance()));
    }

    /**
     * Gets the sequence number of the first event at or after the given timestamp.
     *
     * @param timestampMillis milliseconds since epoch
     * @return the sequence, or -1 if no event exists at or after the given timestamp
     */
    public CompletableFuture<GetSequenceAtResponse> getSequenceAt(long timestampMillis) {
        return GrpcFutures.toCompletableFuture(
                futureStub.getSequenceAt(GetSequenceAtRequest.newBuilder().setTimestamp(timestampMillis).build())
        );
    }

    /**
     * Gets the tags for an event at a specific sequence position.
     */
    public CompletableFuture<GetTagsResponse> getTags(long sequence) {
        return GrpcFutures.toCompletableFuture(
                futureStub.getTags(GetTagsRequest.newBuilder().setSequence(sequence).build())
        );
    }

    /**
     * Represents an in-progress append transaction.
     * Events are buffered and sent as a stream when committed.
     */
    public static class AppendTransaction {

        private final CompletableFuture<AppendResponse> result = new CompletableFuture<>();
        private final StreamObserver<AppendRequest> requestStream;

        AppendTransaction(EventStoreGrpc.EventStoreStub asyncStub) {
            this.requestStream = asyncStub.append(new StreamObserver<>() {
                @Override
                public void onNext(AppendResponse value) {
                    result.complete(value);
                }

                @Override
                public void onError(Throwable t) {
                    result.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    if (!result.isDone()) {
                        result.completeExceptionally(
                                new IllegalStateException("Append stream completed without a response.")
                        );
                    }
                }
            });
        }

        /**
         * Appends events to this transaction with an optional consistency condition.
         */
        public void append(AppendRequest request) {
            requestStream.onNext(request);
        }

        /**
         * Commits the transaction by completing the request stream.
         *
         * @return a future that completes with the append response
         */
        public CompletableFuture<AppendResponse> commit() {
            requestStream.onCompleted();
            return result;
        }

        /**
         * Rolls back the transaction by cancelling the stream.
         */
        public void rollback() {
            requestStream.onError(new RuntimeException("Transaction rolled back by client."));
        }
    }
}
