package io.kronosdb.connector.axon.query;

import io.kronosdb.connector.axon.KronosDbConfiguration;
import io.kronosdb.connector.grpc.KronosDbConnection;
import io.kronosdb.connector.grpc.QueryChannel;
import io.kronosdb.connector.grpc.Registration;
import io.kronosdb.connector.grpc.ResultStream;
import io.kronosdb.grpc.SerializedObject;
import io.kronosdb.grpc.query.QueryRequest;
import io.kronosdb.grpc.query.QueryResponse;
import io.kronosdb.grpc.query.QueryUpdate;
import io.kronosdb.grpc.query.QueryUpdateComplete;
import io.kronosdb.grpc.query.QueryUpdateCompleteExceptionally;
import io.kronosdb.grpc.query.SubscriptionQuery;
import io.kronosdb.grpc.query.SubscriptionQueryResponse;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.distributed.QueryBusConnector;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * A {@link QueryBusConnector} implementation that connects to KronosDB
 * for distributed query dispatching and handling.
 */
public class KronosDbQueryBusConnector implements QueryBusConnector {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbQueryBusConnector.class);

    private final KronosDbConnection connection;
    private final String clientId;
    private final String componentName;
    private final @Nullable MessageConverter converter;

    private final Map<QualifiedName, Registration> subscriptions = new ConcurrentHashMap<>();
    private Handler incomingHandler;

    public KronosDbQueryBusConnector(KronosDbConnection connection,
                                     KronosDbConfiguration configuration) {
        this(connection, configuration, null);
    }

    public KronosDbQueryBusConnector(KronosDbConnection connection,
                                     KronosDbConfiguration configuration,
                                     @Nullable MessageConverter converter) {
        this.connection = requireNonNull(connection);
        requireNonNull(configuration);
        this.clientId = configuration.getClientId();
        this.componentName = configuration.getComponentName();
        this.converter = converter;
    }

    public void start() {
        logger.trace("KronosDbQueryBusConnector started.");
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName name) {
        logger.debug("Subscribing to query handler [{}].", name);

        Registration registration = connection.queryChannel()
                .registerQueryHandler(
                        new LocalSegmentAdapter(),
                        name.fullName(),
                        "",
                        clientId,
                        componentName
                );

        this.subscriptions.put(name, registration);
        CompletableFuture<Void> completion = new CompletableFuture<>();
        registration.onAck(() -> completion.complete(null));
        return completion;
    }

    @Override
    public boolean unsubscribe(QualifiedName name) {
        Registration subscription = subscriptions.remove(name);
        if (subscription != null) {
            subscription.cancel();
            return true;
        }
        return false;
    }

    @Override
    public void onIncomingQuery(Handler handler) {
        this.incomingHandler = requireNonNull(handler);
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
        ResultStream<QueryResponse> resultStream = connection.queryChannel()
                .query(QueryConverter.convertQueryMessage(query, clientId, componentName));
        return new QueryResponseMessageStream(resultStream, converter);
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                  @Nullable ProcessingContext context,
                                                                  int updateBufferSize) {
        QueryChannel.SubscriptionQueryResult result = connection.queryChannel()
                .subscriptionQuery(
                        QueryConverter.convertQueryMessage(query, clientId, componentName),
                        updateBufferSize
                );
        return new SubscriptionQueryResponseMessageStream(result, converter);
    }

    public CompletableFuture<Void> disconnect() {
        if (connection.isConnected()) {
            logger.trace("Disconnecting KronosDbQueryBusConnector.");
            connection.queryChannel().prepareDisconnect();
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> shutdownDispatching() {
        logger.trace("Shutting down dispatching.");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("connection", connection);
        descriptor.describeProperty("clientId", clientId);
        descriptor.describeProperty("componentName", componentName);
    }

    /**
     * Adapts between the KronosDB query handler interface and
     * the Axon Framework query handler.
     */
    private class LocalSegmentAdapter implements QueryChannel.QueryHandler {

        @Override
        public void handle(QueryRequest query,
                           java.util.function.Consumer<QueryResponse> responseSender,
                           Runnable onComplete) {
            var queryMessage = QueryConverter.convertQueryRequest(query, converter);
            var result = incomingHandler.query(queryMessage);

            CompletableFuture.runAsync(() -> {
                try {
                    while (!result.isCompleted()) {
                        result.next().ifPresent(entry -> responseSender.accept(
                                QueryConverter.convertQueryResponseMessage(
                                        query.getMessageIdentifier(), entry.message())));
                    }
                } catch (Exception e) {
                    logger.error("Error handling query [{}].", query.getQuery(), e);
                } finally {
                    onComplete.run();
                    result.close();
                }
            });
        }

        @Override
        public Registration registerSubscriptionQuery(SubscriptionQuery query,
                                                      QueryChannel.SubscriptionUpdateSender updateSender) {
            var queryMessage = QueryConverter.convertSubscriptionQueryMessage(query, converter);

            var registration = incomingHandler.registerUpdateHandler(
                    queryMessage,
                    new KronosDbUpdateCallback(query.getSubscriptionIdentifier(), updateSender)
            );

            return new Registration(() -> {
                registration.cancel();
                updateSender.complete();
            });
        }
    }

    /**
     * Bridges Axon Framework's {@link UpdateCallback} to KronosDB's gRPC update sender.
     * When projections emit updates via {@code QueryUpdateEmitter}, this callback
     * converts them to gRPC messages and sends them back through the handler's OpenStream.
     */
    private class KronosDbUpdateCallback implements UpdateCallback {

        private final String subscriptionId;
        private final QueryChannel.SubscriptionUpdateSender updateSender;

        KronosDbUpdateCallback(String subscriptionId, QueryChannel.SubscriptionUpdateSender updateSender) {
            this.subscriptionId = subscriptionId;
            this.updateSender = updateSender;
        }

        @Override
        public CompletableFuture<Void> sendUpdate(SubscriptionQueryUpdateMessage update) {
            QueryUpdate grpcUpdate = QueryConverter.convertQueryUpdate(update);

            updateSender.sendUpdate(SubscriptionQueryResponse.newBuilder()
                    .setSubscriptionIdentifier(subscriptionId)
                    .setUpdate(grpcUpdate)
                    .build());

            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> complete() {
            updateSender.sendUpdate(SubscriptionQueryResponse.newBuilder()
                    .setSubscriptionIdentifier(subscriptionId)
                    .setComplete(QueryUpdateComplete.newBuilder()
                            .setClientId(clientId)
                            .setComponentName(componentName)
                            .build())
                    .build());
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> completeExceptionally(Throwable error) {
            updateSender.sendUpdate(SubscriptionQueryResponse.newBuilder()
                    .setSubscriptionIdentifier(subscriptionId)
                    .setCompleteExceptionally(QueryUpdateCompleteExceptionally.newBuilder()
                            .setClientId(clientId)
                            .setComponentName(componentName)
                            .setErrorMessage(io.kronosdb.grpc.ErrorMessage.newBuilder()
                                    .setMessage(error.getMessage() != null ? error.getMessage() : "Unknown error")
                                    .build())
                            .build())
                    .build());
            return CompletableFuture.completedFuture(null);
        }
    }
}
