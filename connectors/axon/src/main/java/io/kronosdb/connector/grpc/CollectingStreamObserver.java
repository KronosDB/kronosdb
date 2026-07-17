package io.kronosdb.connector.grpc;

import io.grpc.stub.StreamObserver;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StreamObserver} that collects streamed responses into a blocking queue,
 * making them available for pull-based consumption via {@link ResultStream}.
 */
public class CollectingStreamObserver<T> implements StreamObserver<T> {

    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();
    private final CompletableFuture<Void> completionFuture;
    private volatile @Nullable Throwable error;
    private volatile boolean completed = false;
    private volatile @Nullable Runnable onAvailableCallback;

    CollectingStreamObserver(CompletableFuture<Void> completionFuture) {
        this.completionFuture = completionFuture;
    }

    @Override
    public void onNext(T value) {
        queue.add(value);
        Runnable callback = onAvailableCallback;
        if (callback != null) {
            callback.run();
        }
    }

    @Override
    public void onError(Throwable t) {
        this.error = t;
        this.completed = true;
        completionFuture.completeExceptionally(t);
        Runnable callback = onAvailableCallback;
        if (callback != null) {
            callback.run();
        }
    }

    @Override
    public void onCompleted() {
        this.completed = true;
        completionFuture.complete(null);
        Runnable callback = onAvailableCallback;
        if (callback != null) {
            callback.run();
        }
    }

    @Nullable
    T poll() {
        return queue.poll();
    }

    @Nullable
    T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Nullable
    T peek() {
        return queue.peek();
    }

    boolean hasNext() {
        return !queue.isEmpty();
    }

    boolean isCompleted() {
        return completed && queue.isEmpty();
    }

    boolean isClosed() {
        return completed;
    }

    @Nullable
    Throwable getError() {
        return error;
    }

    void onAvailable(Runnable callback) {
        this.onAvailableCallback = callback;
        // If items are already available, fire immediately
        if (!queue.isEmpty() || completed) {
            callback.run();
        }
    }
}
