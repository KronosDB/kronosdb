package io.kronosdb.connector.grpc;

import io.grpc.ManagedChannel;
import io.kronosdb.grpc.command.CommandServiceGrpc;
import io.kronosdb.grpc.eventstore.EventStoreGrpc;
import io.kronosdb.grpc.platform.PlatformServiceGrpc;
import io.kronosdb.grpc.query.QueryServiceGrpc;
import io.kronosdb.grpc.snapshot.SnapshotStoreGrpc;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Represents a connection to a single KronosDB server for a specific context.
 * Provides access to typed channel abstractions for event store, snapshot store,
 * command bus, query bus, and platform operations.
 */
public class KronosDbConnection {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbConnection.class);

    private final ManagedChannel channel;
    private final String context;

    private volatile @Nullable EventStoreChannel eventStoreChannel;
    private volatile @Nullable SnapshotChannel snapshotChannel;
    private volatile @Nullable CommandChannel commandChannel;
    private volatile @Nullable QueryChannel queryChannel;
    private volatile @Nullable PlatformChannel platformChannel;

    KronosDbConnection(ManagedChannel channel, String context) {
        this.channel = Objects.requireNonNull(channel, "The ManagedChannel must not be null.");
        this.context = Objects.requireNonNull(context, "The context must not be null.");
    }

    /**
     * Returns the event store channel for this connection.
     */
    public EventStoreChannel eventStoreChannel() {
        if (eventStoreChannel == null) {
            synchronized (this) {
                if (eventStoreChannel == null) {
                    eventStoreChannel = new EventStoreChannel(
                            EventStoreGrpc.newStub(channel),
                            EventStoreGrpc.newBlockingStub(channel),
                            EventStoreGrpc.newFutureStub(channel),
                            context
                    );
                }
            }
        }
        return eventStoreChannel;
    }

    /**
     * Returns the snapshot channel for this connection.
     */
    public SnapshotChannel snapshotChannel() {
        if (snapshotChannel == null) {
            synchronized (this) {
                if (snapshotChannel == null) {
                    snapshotChannel = new SnapshotChannel(
                            SnapshotStoreGrpc.newStub(channel),
                            SnapshotStoreGrpc.newBlockingStub(channel),
                            SnapshotStoreGrpc.newFutureStub(channel),
                            context
                    );
                }
            }
        }
        return snapshotChannel;
    }

    /**
     * Returns the command channel for this connection.
     */
    public CommandChannel commandChannel() {
        if (commandChannel == null) {
            synchronized (this) {
                if (commandChannel == null) {
                    commandChannel = new CommandChannel(
                            CommandServiceGrpc.newStub(channel),
                            CommandServiceGrpc.newBlockingStub(channel),
                            CommandServiceGrpc.newFutureStub(channel),
                            context
                    );
                }
            }
        }
        return commandChannel;
    }

    /**
     * Returns the query channel for this connection.
     */
    public QueryChannel queryChannel() {
        if (queryChannel == null) {
            synchronized (this) {
                if (queryChannel == null) {
                    queryChannel = new QueryChannel(
                            QueryServiceGrpc.newStub(channel),
                            QueryServiceGrpc.newBlockingStub(channel),
                            QueryServiceGrpc.newFutureStub(channel),
                            context
                    );
                }
            }
        }
        return queryChannel;
    }

    /**
     * Returns the platform channel for this connection.
     */
    public PlatformChannel platformChannel() {
        if (platformChannel == null) {
            synchronized (this) {
                if (platformChannel == null) {
                    platformChannel = new PlatformChannel(
                            PlatformServiceGrpc.newStub(channel),
                            PlatformServiceGrpc.newBlockingStub(channel),
                            PlatformServiceGrpc.newFutureStub(channel),
                            context
                    );
                }
            }
        }
        return platformChannel;
    }

    /**
     * Returns the context this connection is associated with.
     */
    public String context() {
        return context;
    }

    /**
     * Returns whether this connection's channel is not shut down.
     */
    public boolean isConnected() {
        return !channel.isShutdown() && !channel.isTerminated();
    }

    /**
     * Disconnects this connection, shutting down the underlying gRPC channel.
     */
    public void disconnect() {
        logger.info("Disconnecting KronosDB connection for context [{}].", context);
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Channel for context [{}] did not terminate in time, forcing shutdown.", context);
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
