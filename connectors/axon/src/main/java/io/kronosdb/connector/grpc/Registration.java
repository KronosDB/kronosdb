package io.kronosdb.connector.grpc;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a registration (e.g., a command handler subscription) that can be cancelled.
 * Supports server-side acknowledgement tracking via a {@link CompletableFuture}.
 */
public class Registration {

    private final Runnable cancelAction;
    private final CompletableFuture<Void> ackFuture;

    public Registration(Runnable cancelAction) {
        this(cancelAction, new CompletableFuture<>());
    }

    public Registration(Runnable cancelAction, CompletableFuture<Void> ackFuture) {
        this.cancelAction = cancelAction;
        this.ackFuture = ackFuture;
    }

    /**
     * Cancels this registration.
     */
    public CompletableFuture<Void> cancel() {
        cancelAction.run();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Registers a callback to be invoked when the server acknowledges this registration.
     * If the ack has already been received, the callback fires immediately.
     */
    public void onAck(Runnable callback) {
        ackFuture.thenRun(callback);
    }

    /**
     * Returns the underlying ack future. Completes when the server ack is received.
     */
    public CompletableFuture<Void> ackFuture() {
        return ackFuture;
    }
}
