package io.kronosdb.connector.axon;

import io.kronosdb.connector.grpc.KronosDbConnection;
import io.kronosdb.connector.grpc.KronosDbConnectionFactory;
import io.kronosdb.grpc.platform.ClientIdentification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages all connections between an Axon Framework application and KronosDB.
 * Creates and maintains connections per context, providing them for the various
 * connectors (event store, command bus, query bus, snapshot store).
 * <p>
 * Provides connection lifecycle management including heartbeat and reconnection.
 */
public class KronosDbConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbConnectionManager.class);

    private final Map<String, KronosDbConnection> connections = new ConcurrentHashMap<>();
    private final KronosDbConnectionFactory connectionFactory;
    private final KronosDbConfiguration configuration;
    private final String defaultContext;

    private KronosDbConnectionManager(Builder builder) {
        this.configuration = builder.configuration;
        this.defaultContext = configuration.getContext();

        KronosDbConnectionFactory.Builder factoryBuilder =
                KronosDbConnectionFactory.forClient(configuration.getComponentName(), configuration.getClientId());

        // Parse server address
        String servers = configuration.getServers();
        String[] parts = servers.split(":");
        factoryBuilder.host(parts[0]);
        if (parts.length > 1) {
            factoryBuilder.port(Integer.parseInt(parts[1]));
        }

        if (configuration.getToken() != null) {
            factoryBuilder.token(configuration.getToken());
        }

        if (configuration.isSslEnabled()) {
            try {
                if (configuration.getCertFile() != null) {
                    factoryBuilder.useTls(new File(configuration.getCertFile()));
                } else {
                    factoryBuilder.useTls();
                }
            } catch (SSLException e) {
                throw new RuntimeException("Failed to configure TLS for KronosDB connection.", e);
            }
        }

        if (configuration.getKeepAliveTime() > 0) {
            factoryBuilder.keepAlive(configuration.getKeepAliveTime(), configuration.getKeepAliveTimeout());
        }

        if (configuration.getMaxMessageSize() > 0) {
            factoryBuilder.maxInboundMessageSize(configuration.getMaxMessageSize());
        }

        this.connectionFactory = factoryBuilder.build();
    }

    /**
     * Creates a new builder for the connection manager.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Starts the connection manager. Eagerly establishes the platform stream
     * (heartbeat, lifecycle) for the default context, but command/query/event
     * streams are created lazily on first use.
     */
    public void start() {
        logger.info("Starting KronosDB connection manager for [{}].", defaultContext);
        // Eagerly register the platform stream in the background so that
        // start() does not block the application bootstrap.
        CompletableFuture.runAsync(() -> {
            try {
                getConnection();
            } catch (Exception e) {
                logger.warn("Eager platform connection for context [{}] failed — will retry on first use: {}.",
                        defaultContext, e.getMessage());
            }
        });
    }

    /**
     * Returns the connection for the default context.
     */
    public KronosDbConnection getConnection() {
        return getConnection(defaultContext);
    }

    /**
     * Returns the connection for the given context.
     */
    public KronosDbConnection getConnection(String context) {
        return connections.computeIfAbsent(context, ctx -> {
            KronosDbConnection conn = connectionFactory.connect(ctx);
            registerWithPlatform(conn);
            return conn;
        });
    }

    private void registerWithPlatform(KronosDbConnection conn) {
        ClientIdentification identification = ClientIdentification.newBuilder()
                .setClientId(configuration.getClientId())
                .setComponentName(configuration.getComponentName())
                .setVersion("0.1.0")
                .build();

        // Unary RPC to register client
        conn.platformChannel().connect(identification)
                .whenComplete((info, error) -> {
                    if (error != null) {
                        logger.error("Failed to register with KronosDB platform service.", error);
                    } else {
                        logger.info("Registered with KronosDB node [{}].", info.getNodeName());
                    }
                });

        // Open the persistent bidi stream for heartbeat and lifecycle
        conn.platformChannel().openStream(identification, () ->
                logger.info("KronosDB requested reconnect for context [{}].", conn.context()));

        if (configuration.isHeartbeatEnabled()) {
            conn.platformChannel().enableHeartbeat(
                    configuration.getHeartbeatInterval(),
                    configuration.getHeartbeatTimeout()
            );
        }
    }

    /**
     * Returns whether a connection exists and is active for the given context.
     */
    public boolean isConnected(String context) {
        KronosDbConnection conn = connections.get(context);
        return conn != null && conn.isConnected();
    }

    /**
     * Returns the default context name.
     */
    public String getDefaultContext() {
        return defaultContext;
    }

    /**
     * Returns a snapshot of all connections and their connected status.
     */
    public Map<String, Boolean> connections() {
        return connections.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().isConnected()));
    }

    /**
     * Disconnects the given context.
     */
    public void disconnect(String context) {
        KronosDbConnection conn = connections.remove(context);
        if (conn != null) {
            conn.disconnect();
        }
    }

    /**
     * Shuts down the connection manager and all connections.
     */
    public void shutdown() {
        logger.info("Shutting down KronosDB connection manager.");
        connectionFactory.shutdown();
        connections.forEach((ctx, conn) -> conn.disconnect());
        connections.clear();
    }

    /**
     * Builder for {@link KronosDbConnectionManager}.
     */
    public static class Builder {

        private KronosDbConfiguration configuration;

        public Builder configuration(KronosDbConfiguration configuration) {
            this.configuration = configuration;
            return this;
        }

        public KronosDbConnectionManager build() {
            if (configuration == null) {
                throw new IllegalStateException("KronosDbConfiguration is required.");
            }
            return new KronosDbConnectionManager(this);
        }
    }
}
