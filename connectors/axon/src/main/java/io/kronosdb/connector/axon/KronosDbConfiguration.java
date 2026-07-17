package io.kronosdb.connector.axon;

import org.jspecify.annotations.Nullable;

import java.util.UUID;

/**
 * Configuration for connecting an Axon Framework application to a KronosDB instance.
 * <p>
 * Provides all settings needed to establish and maintain a connection to KronosDB.
 */
public class KronosDbConfiguration {

    private String servers = "localhost:50051";
    private String context = "default";
    private String clientId = UUID.randomUUID().toString();
    private String componentName = "Unnamed";
    private @Nullable String token;
    private boolean sslEnabled = false;
    private @Nullable String certFile;
    private int commandLoadFactor = 100;

    // Flow control
    private int commandFlowControlPermits = 5000;
    private int queryFlowControlPermits = 5000;

    // Keep-alive
    private long keepAliveTime = 1000;
    private long keepAliveTimeout = 5000;

    // Heartbeat
    private boolean heartbeatEnabled = true;
    private long heartbeatInterval = 5000;
    private long heartbeatTimeout = 15000;

    // Connection
    private long connectTimeout = 5000;
    private long reconnectInterval = 2000;

    // Message size
    private int maxMessageSize = 4 * 1024 * 1024; // 4MB

    public KronosDbConfiguration() {
    }

    // --- Getters and setters ---

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    @Nullable
    public String getToken() {
        return token;
    }

    public void setToken(@Nullable String token) {
        this.token = token;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    @Nullable
    public String getCertFile() {
        return certFile;
    }

    public void setCertFile(@Nullable String certFile) {
        this.certFile = certFile;
    }

    public int getCommandLoadFactor() {
        return commandLoadFactor;
    }

    public void setCommandLoadFactor(int commandLoadFactor) {
        this.commandLoadFactor = commandLoadFactor;
    }

    public int getCommandFlowControlPermits() {
        return commandFlowControlPermits;
    }

    public void setCommandFlowControlPermits(int commandFlowControlPermits) {
        this.commandFlowControlPermits = commandFlowControlPermits;
    }

    public int getQueryFlowControlPermits() {
        return queryFlowControlPermits;
    }

    public void setQueryFlowControlPermits(int queryFlowControlPermits) {
        this.queryFlowControlPermits = queryFlowControlPermits;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public long getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public void setKeepAliveTimeout(long keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
    }

    public boolean isHeartbeatEnabled() {
        return heartbeatEnabled;
    }

    public void setHeartbeatEnabled(boolean heartbeatEnabled) {
        this.heartbeatEnabled = heartbeatEnabled;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(long heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public long getReconnectInterval() {
        return reconnectInterval;
    }

    public void setReconnectInterval(long reconnectInterval) {
        this.reconnectInterval = reconnectInterval;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    /**
     * Convenience builder for creating configurations programmatically.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final KronosDbConfiguration config = new KronosDbConfiguration();

        public Builder servers(String servers) {
            config.setServers(servers);
            return this;
        }

        public Builder context(String context) {
            config.setContext(context);
            return this;
        }

        public Builder clientId(String clientId) {
            config.setClientId(clientId);
            return this;
        }

        public Builder componentName(String componentName) {
            config.setComponentName(componentName);
            return this;
        }

        public Builder token(String token) {
            config.setToken(token);
            return this;
        }

        public Builder sslEnabled(boolean sslEnabled) {
            config.setSslEnabled(sslEnabled);
            return this;
        }

        public Builder certFile(String certFile) {
            config.setCertFile(certFile);
            return this;
        }

        public Builder commandLoadFactor(int loadFactor) {
            config.setCommandLoadFactor(loadFactor);
            return this;
        }

        public Builder keepAlive(long timeMillis, long timeoutMillis) {
            config.setKeepAliveTime(timeMillis);
            config.setKeepAliveTimeout(timeoutMillis);
            return this;
        }

        public Builder heartbeat(boolean enabled, long intervalMillis, long timeoutMillis) {
            config.setHeartbeatEnabled(enabled);
            config.setHeartbeatInterval(intervalMillis);
            config.setHeartbeatTimeout(timeoutMillis);
            return this;
        }

        public Builder maxMessageSize(int size) {
            config.setMaxMessageSize(size);
            return this;
        }

        public KronosDbConfiguration build() {
            return config;
        }
    }
}
