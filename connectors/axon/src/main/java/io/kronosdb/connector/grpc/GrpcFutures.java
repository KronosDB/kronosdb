package io.kronosdb.connector.grpc;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Utility to bridge Guava's {@link ListenableFuture} (used by gRPC future stubs)
 * to Java's {@link CompletableFuture}.
 */
public final class GrpcFutures {

    private GrpcFutures() {
    }

    /**
     * Converts a Guava {@link ListenableFuture} to a Java {@link CompletableFuture}.
     */
    public static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> listenableFuture) {
        CompletableFuture<T> future = new CompletableFuture<>();
        listenableFuture.addListener(() -> {
            try {
                future.complete(listenableFuture.get());
            } catch (ExecutionException e) {
                future.completeExceptionally(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }, Runnable::run);
        return future;
    }
}
