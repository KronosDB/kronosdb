package io.kronosdb.connector.grpc;

import io.grpc.stub.StreamObserver;
import io.kronosdb.grpc.eventstore.StreamControl;
import io.kronosdb.grpc.eventstore.StreamPermits;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

/**
 * A {@link ResultStream} with automatic permit-based flow control.
 *
 * <p>Tracks how many permits the consumed messages actually cost and
 * automatically grants more permits to the server when the remaining count
 * drops below a threshold. Permits count EVENTS, not messages: a batched
 * message costs one permit per contained event and a heartbeat costs zero,
 * so the cost is computed per message via {@code permitCost}.</p>
 *
 * @param <T> the type of elements in this stream
 */
public class FlowControlledResultStream<T> extends ResultStream<T> {

    private static final double REFILL_THRESHOLD = 0.25;

    private final StreamObserver<StreamControl> requestStream;
    private final long permitBatchSize;
    private final AtomicLong remainingPermits;
    private final ToLongFunction<T> permitCost;

    FlowControlledResultStream(CollectingStreamObserver<T> observer,
                                CompletableFuture<Void> completionFuture,
                                StreamObserver<StreamControl> requestStream,
                                long permitBatchSize,
                                ToLongFunction<T> permitCost) {
        super(observer, completionFuture);
        this.requestStream = requestStream;
        this.permitBatchSize = permitBatchSize;
        this.remainingPermits = new AtomicLong(permitBatchSize);
        this.permitCost = permitCost;
    }

    @Override
    @Nullable
    public T nextIfAvailable() {
        T item = super.nextIfAvailable();
        if (item != null) {
            onConsumed(permitCost.applyAsLong(item));
        }
        return item;
    }

    @Override
    @Nullable
    public T nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        T item = super.nextIfAvailable(timeout, unit);
        if (item != null) {
            onConsumed(permitCost.applyAsLong(item));
        }
        return item;
    }

    private void onConsumed(long cost) {
        if (cost <= 0) {
            return; // Heartbeats and empty batches consume no permits.
        }
        long remaining = remainingPermits.addAndGet(-cost);
        long threshold = (long) (permitBatchSize * REFILL_THRESHOLD);
        if (remaining <= threshold && !isClosed()) {
            long grant = permitBatchSize - remaining;
            remainingPermits.addAndGet(grant);
            try {
                requestStream.onNext(
                        StreamControl.newBuilder()
                                .setPermits(StreamPermits.newBuilder().setPermits(grant).build())
                                .build()
                );
            } catch (Exception e) {
                // Stream may have been closed — ignore.
            }
        }
    }

    @Override
    public void close() {
        try {
            requestStream.onCompleted();
        } catch (Exception e) {
            // Already closed — ignore.
        }
        super.close();
    }
}
