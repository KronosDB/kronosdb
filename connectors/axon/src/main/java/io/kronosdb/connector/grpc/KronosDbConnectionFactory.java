package io.kronosdb.connector.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import javax.net.ssl.SSLException;

/**
 * Factory for creating {@link KronosDbConnection} instances.
 * Manages the underlying gRPC channels and their configuration.
 */
public class KronosDbConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbConnectionFactory.class);
    private static final int DEFAULT_PORT = 50051;

    private final String host;
    private final int port;
    private final String clientId;
    private final String componentName;
    private final @Nullable String busName;
    private final @Nullable String token;
    private final @Nullable SslContext sslContext;
    private final long keepAliveTimeMillis;
    private final long keepAliveTimeoutMillis;
    private final int maxInboundMessageSize;
    private final @Nullable UnaryOperator<ManagedChannelBuilder<?>> channelCustomizer;

    private final Map<String, KronosDbConnection> connections = new ConcurrentHashMap<>();

    private KronosDbConnectionFactory(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.clientId = builder.clientId;
        this.componentName = builder.componentName;
        this.busName = builder.busName;
        this.token = builder.token;
        this.sslContext = builder.sslContext;
        this.keepAliveTimeMillis = builder.keepAliveTimeMillis;
        this.keepAliveTimeoutMillis = builder.keepAliveTimeoutMillis;
        this.maxInboundMessageSize = builder.maxInboundMessageSize;
        this.channelCustomizer = builder.channelCustomizer;
    }

    /**
     * Creates a new builder for configuring a connection factory.
     */
    public static Builder forClient(String componentName, String clientId) {
        return new Builder(componentName, clientId);
    }

    /**
     * Creates or retrieves a connection for the given context.
     */
    public KronosDbConnection connect(String context) {
        return connections.computeIfAbsent(context, this::createConnection);
    }

    private KronosDbConnection createConnection(String context) {
        logger.info("Creating KronosDB connection to [{}:{}] for context [{}].", host, port, context);

        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(host, port);

        if (sslContext != null) {
            channelBuilder.sslContext(sslContext);
        } else {
            channelBuilder.usePlaintext();
        }

        if (keepAliveTimeMillis > 0) {
            channelBuilder.keepAliveTime(keepAliveTimeMillis, TimeUnit.MILLISECONDS)
                          .keepAliveTimeout(keepAliveTimeoutMillis, TimeUnit.MILLISECONDS)
                          .keepAliveWithoutCalls(true);
        }

        if (maxInboundMessageSize > 0) {
            channelBuilder.maxInboundMessageSize(maxInboundMessageSize);
        }

        // Add context, bus, and token metadata interceptor. The bus routes
        // messaging (commands/queries/subscriptions) independently of the
        // event store context; unset means the server's default bus.
        channelBuilder.intercept(new ContextMetadata(
                context,
                busName != null ? busName : "",
                token != null ? token : ""));

        if (channelCustomizer != null) {
            channelCustomizer.apply(channelBuilder);
        }

        ManagedChannel channel = channelBuilder.build();
        return new KronosDbConnection(channel, context);
    }

    /**
     * Shuts down all connections.
     */
    public void shutdown() {
        connections.values().forEach(KronosDbConnection::disconnect);
        connections.clear();
    }

    public String clientId() {
        return clientId;
    }

    public String componentName() {
        return componentName;
    }

    /**
     * Builder for {@link KronosDbConnectionFactory}.
     */
    public static class Builder {

        private String host = "localhost";
        private int port = DEFAULT_PORT;
        private final String clientId;
        private final String componentName;
        private @Nullable String busName;
        private @Nullable String token;
        private @Nullable SslContext sslContext;
        private long keepAliveTimeMillis = 10_000;   // 10s ping interval, conventional for long-lived streams
        private long keepAliveTimeoutMillis = 5_000;
        private int maxInboundMessageSize = 0;
        private @Nullable UnaryOperator<ManagedChannelBuilder<?>> channelCustomizer;

        Builder(String componentName, String clientId) {
            this.componentName = Objects.requireNonNull(componentName);
            this.clientId = Objects.requireNonNull(clientId);
        }

        public Builder host(String host) {
            this.host = Objects.requireNonNull(host);
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Names the messaging bus for command, query, and subscription-query
         * calls. Buses are independent of event store contexts: connections
         * to different contexts share one bus when given the same name.
         * Unset routes messaging to the server's {@code default} bus.
         */
        public Builder busName(String busName) {
            this.busName = busName;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder useTls(File certFile) throws SSLException {
            this.sslContext = GrpcSslContexts.forClient()
                    .trustManager(certFile)
                    .build();
            return this;
        }

        public Builder useTls() throws SSLException {
            this.sslContext = GrpcSslContexts.forClient().build();
            return this;
        }

        public Builder keepAlive(long timeMillis, long timeoutMillis) {
            this.keepAliveTimeMillis = timeMillis;
            this.keepAliveTimeoutMillis = timeoutMillis;
            return this;
        }

        public Builder maxInboundMessageSize(int size) {
            this.maxInboundMessageSize = size;
            return this;
        }

        public Builder channelCustomizer(UnaryOperator<ManagedChannelBuilder<?>> customizer) {
            this.channelCustomizer = customizer;
            return this;
        }

        public KronosDbConnectionFactory build() {
            return new KronosDbConnectionFactory(this);
        }
    }
}
