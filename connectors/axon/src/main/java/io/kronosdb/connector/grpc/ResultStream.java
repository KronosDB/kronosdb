package io.kronosdb.connector.grpc;

import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * An async pull-based result stream backed by a gRPC server-streaming call.
 * Provides non-blocking polling and callback-based notification when new items arrive.
 *
 * @param <T> the type of elements in this stream
 */
public class ResultStream<T> implements AutoCloseable {

    private final CollectingStreamObserver<T> observer;
    private final CompletableFuture<Void> completionFuture;

    ResultStream(CollectingStreamObserver<T> observer, CompletableFuture<Void> completionFuture) {
        this.observer = observer;
        this.completionFuture = completionFuture;
    }

    /**
     * Returns the next available element, or {@code null} if none is immediately available.
     */
    @Nullable
    public T nextIfAvailable() {
        return observer.poll();
    }

    /**
     * Waits up to the specified time for the next element.
     */
    @Nullable
    public T nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        return observer.poll(timeout, unit);
    }

    /**
     * Peeks at the next element without consuming it.
     */
    @Nullable
    public T peek() {
        return observer.peek();
    }

    /**
     * Returns whether there is at least one element available for immediate consumption.
     */
    public boolean hasNext() {
        return observer.hasNext();
    }

    /**
     * Returns whether this stream has been fully consumed (completed and queue empty).
     */
    public boolean isCompleted() {
        return observer.isCompleted();
    }

    /**
     * Returns whether the server has closed its side of the stream.
     */
    public boolean isClosed() {
        return observer.isClosed();
    }

    /**
     * Returns the error that terminated the stream, if any.
     */
    public Optional<Throwable> getError() {
        return Optional.ofNullable(observer.getError());
    }

    /**
     * Registers a callback to be invoked when new elements become available
     * or the stream completes/errors.
     */
    public void onAvailable(Runnable callback) {
        observer.onAvailable(callback);
    }

    @Override
    public void close() {
        completionFuture.cancel(true);
    }
}
