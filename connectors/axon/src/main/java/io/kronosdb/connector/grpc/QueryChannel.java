package io.kronosdb.connector.grpc;

import io.grpc.stub.StreamObserver;
import io.kronosdb.grpc.FlowControl;
import io.kronosdb.grpc.InstructionAck;
import io.kronosdb.grpc.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Channel abstraction for KronosDB QueryService gRPC operations.
 * Manages the bidirectional stream for query handler registration and
 * provides dispatch capability.
 */
public class QueryChannel {

    private static final Logger logger = LoggerFactory.getLogger(QueryChannel.class);

    // Reconnection backoff constants
    private static final long BACKOFF_INITIAL_MS = 500;
    private static final long BACKOFF_MAX_MS = 30_000;
    private static final double BACKOFF_JITTER_FACTOR = 0.25;

    // Flow control constants
    private static final long INITIAL_PERMITS = 500;
    private static final double REFILL_THRESHOLD = 0.25;

    // Keep-alive interval


    private final QueryServiceGrpc.QueryServiceStub asyncStub;
    private final QueryServiceGrpc.QueryServiceBlockingStub blockingStub;
    private final QueryServiceGrpc.QueryServiceFutureStub futureStub;
    private final String context;

    private volatile StreamObserver<QueryHandlerOutbound> handlerStream;
    // Guards all handlerStream.onNext() calls — StreamObserver is NOT thread-safe.
    // Without this, concurrent query responses and subscription updates corrupt the
    // gRPC output buffer, producing "invalid tag value: 0" decode errors on the server.
    private final Object streamWriteLock = new Object();
    private final Map<String, CompletableFuture<InstructionAck>> pendingAcks = new ConcurrentHashMap<>();
    private QueryHandler queryHandler;

    // Tracked state for reconnection.
    private volatile String registeredClientId;
    private volatile String registeredComponentName;
    private final ConcurrentHashMap<String, QuerySubscription> activeSubscriptions = new ConcurrentHashMap<>();
    private volatile boolean reconnecting = false;

    // Adaptive flow control
    private final AtomicLong permitsRemaining = new AtomicLong(0);
    private volatile long currentPermitBatch = INITIAL_PERMITS;

    // Keep-alive scheduler
    // Keep-alive is handled by gRPC HTTP/2 PING frames (configured on the ManagedChannel).
    // No application-level keep-alive needed on handler streams.

    QueryChannel(QueryServiceGrpc.QueryServiceStub asyncStub,
                 QueryServiceGrpc.QueryServiceBlockingStub blockingStub,
                 QueryServiceGrpc.QueryServiceFutureStub futureStub,
                 String context) {
        this.asyncStub = asyncStub;
        this.blockingStub = blockingStub;
        this.futureStub = futureStub;
        this.context = context;
    }

    /**
     * Dispatches a query and returns a stream of results.
     */
    public ResultStream<QueryResponse> query(QueryRequest request) {
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        CollectingStreamObserver<QueryResponse> observer = new CollectingStreamObserver<>(completionFuture);
        asyncStub.query(request, observer);
        return new ResultStream<>(observer, completionFuture);
    }

    /**
     * Opens a subscription query for initial result plus live updates.
     */
    public SubscriptionQueryResult subscriptionQuery(QueryRequest queryRequest, int bufferSize, int refillBatch) {
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        CollectingStreamObserver<SubscriptionQueryResponse> observer = new CollectingStreamObserver<>(completionFuture);

        String subscriptionId = UUID.randomUUID().toString();
        StreamObserver<SubscriptionQueryRequest> requestStream = asyncStub.subscription(observer);

        // Send subscribe message
        requestStream.onNext(SubscriptionQueryRequest.newBuilder()
                .setSubscribe(SubscriptionQuery.newBuilder()
                        .setSubscriptionIdentifier(subscriptionId)
                        .setNumberOfPermits(bufferSize)
                        .setQueryRequest(queryRequest)
                        .build())
                .build());

        return new SubscriptionQueryResult(
                new ResultStream<>(observer, completionFuture),
                requestStream,
                subscriptionId
        );
    }

    /**
     * Registers a query handler for the given query definition.
     */
    public Registration registerQueryHandler(QueryHandler handler, String queryName, String resultName,
                                                String clientId, String componentName) {
        this.queryHandler = handler;
        this.registeredClientId = clientId;
        this.registeredComponentName = componentName;

        ensureHandlerStreamOpen();

        QuerySubscription subscription = QuerySubscription.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .setQuery(queryName)
                .setResultName(resultName)
                .setClientId(clientId)
                .setComponentName(componentName)
                .build();

        activeSubscriptions.put(queryName, subscription);
        CompletableFuture<InstructionAck> ackFuture = sendSubscription(subscription);

        // Grant initial permits
        grantPermits(clientId, INITIAL_PERMITS);
        permitsRemaining.set(INITIAL_PERMITS);
        currentPermitBatch = INITIAL_PERMITS;

        // Start keep-alive


        // Build the registration ack future from the subscription ack
        CompletableFuture<Void> registrationAck;
        if (ackFuture != null) {
            registrationAck = ackFuture.thenApply(ack -> null);
        } else {
            registrationAck = CompletableFuture.completedFuture(null);
        }

        return new Registration(() -> {
            activeSubscriptions.remove(queryName);
            sendToStream(QueryHandlerOutbound.newBuilder()
                    .setUnsubscribe(subscription)
                    .build());
            if (activeSubscriptions.isEmpty()) {

            }
        }, registrationAck);
    }

    /**
     * Thread-safe write to the handler stream.
     */
    private void sendToStream(QueryHandlerOutbound message) {
        StreamObserver<QueryHandlerOutbound> stream = handlerStream;
        if (stream != null) {
            synchronized (streamWriteLock) {
                stream.onNext(message);
            }
        }
    }

    private CompletableFuture<InstructionAck> sendSubscription(QuerySubscription subscription) {
        if (handlerStream != null) {
            String instructionId = UUID.randomUUID().toString();
            CompletableFuture<InstructionAck> ackFuture = new CompletableFuture<>();
            pendingAcks.put(instructionId, ackFuture);

            sendToStream(QueryHandlerOutbound.newBuilder()
                    .setSubscribe(subscription)
                    .setInstructionId(instructionId)
                    .build());

            // Time out the ack after 5 seconds
            ackFuture.orTimeout(5, TimeUnit.SECONDS)
                    .exceptionally(ex -> {
                        pendingAcks.remove(instructionId);
                        logger.debug("Ack timeout for query subscription [{}] instruction [{}].",
                                subscription.getQuery(), instructionId);
                        return InstructionAck.newBuilder()
                                .setInstructionId(instructionId)
                                .setSuccess(true)
                                .build();
                    });
            return ackFuture;
        }
        return null;
    }

    private void grantPermits(String clientId, long permits) {
        sendToStream(QueryHandlerOutbound.newBuilder()
                .setFlowControl(FlowControl.newBuilder()
                        .setClientId(clientId)
                        .setPermits(permits)
                        .build())
                .build());
    }

    /**
     * Checks if permits are running low and refills proactively.
     */
    private void maybeRefillPermits() {
        long remaining = permitsRemaining.get();
        long threshold = (long) (currentPermitBatch * REFILL_THRESHOLD);
        if (remaining <= threshold && registeredClientId != null) {
            long refill = currentPermitBatch;
            permitsRemaining.addAndGet(refill);
            grantPermits(registeredClientId, refill);
            logger.debug("Refilled {} query permits for context [{}]. Remaining: {}.",
                    refill, context, permitsRemaining.get());
        }
    }

    private synchronized void ensureHandlerStreamOpen() {
        if (handlerStream != null) {
            return;
        }

        handlerStream = asyncStub.openStream(new StreamObserver<>() {
            @Override
            public void onNext(QueryHandlerInbound inbound) {
                
                if (inbound.hasQuery()) {
                    permitsRemaining.decrementAndGet();
                    handleIncomingQuery(inbound.getQuery(), inbound.getInstructionId());
                    maybeRefillPermits();
                } else if (inbound.hasAck()) {
                    handleAck(inbound.getAck());
                } else if (inbound.hasSubscriptionQueryRequest()) {
                    handleSubscriptionQuery(inbound.getSubscriptionQueryRequest());
                } else if (inbound.hasQueryCancel()) {
                    // Client cancelled the query
                } else if (inbound.hasQueryFlowControl()) {
                    // Flow control for query results
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Query handler stream error for context [{}]: {}.", context, t.getMessage());
                handlerStream = null;

                scheduleReconnect();
            }

            @Override
            public void onCompleted() {
                logger.info("Query handler stream completed for context [{}].", context);
                handlerStream = null;

                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (activeSubscriptions.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (reconnecting) {
                return;
            }
            reconnecting = true;
        }

        CompletableFuture.runAsync(() -> {
            int attempt = 0;
            try {
                while (!activeSubscriptions.isEmpty()) {
                    attempt++;
                    long delay = calculateBackoff(attempt);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    try {
                        logger.info("Reconnecting query handler stream for context [{}] (attempt {}, delay {}ms).",
                                context, attempt, delay);
                        ensureHandlerStreamOpen();

                        if (handlerStream == null) {
                            logger.warn("Stream open returned but handlerStream is null — retrying.");
                            continue;
                        }

                        for (QuerySubscription sub : activeSubscriptions.values()) {
                            sendSubscription(sub);
                        }
                        if (registeredClientId != null) {
                            grantPermits(registeredClientId, INITIAL_PERMITS);
                            permitsRemaining.set(INITIAL_PERMITS);
                        }

                        if (handlerStream == null) {
                            logger.warn("Stream died during re-registration for context [{}] — retrying.", context);
                            continue;
                        }

                        logger.info("Query handler stream reconnected for context [{}] with {} subscriptions.",
                                context, activeSubscriptions.size());
                        return;
                    } catch (Exception e) {
                        logger.warn("Query handler reconnect attempt {} failed for context [{}]: {}.",
                                attempt, context, e.getMessage());
                        handlerStream = null;
                    }
                }
            } finally {
                reconnecting = false;
            }
        });
    }

    /**
     * Calculates exponential backoff with jitter.
     */
    static long calculateBackoff(int attempt) {
        long delay = Math.min(BACKOFF_INITIAL_MS * (1L << (attempt - 1)), BACKOFF_MAX_MS);
        long jitter = (long) (delay * BACKOFF_JITTER_FACTOR * ThreadLocalRandom.current().nextDouble());
        return delay + jitter;
    }



    private void handleAck(InstructionAck ack) {
        CompletableFuture<InstructionAck> future = pendingAcks.remove(ack.getInstructionId());
        if (future != null) {
            future.complete(ack);
        }
    }

    private void handleIncomingQuery(QueryRequest query, String instructionId) {
        if (queryHandler == null) {
            logger.warn("Received query [{}] but no handler is registered.", query.getQuery());
            return;
        }

        queryHandler.handle(query, response -> {
            sendToStream(QueryHandlerOutbound.newBuilder()
                    .setQueryResponse(response)
                    .build());
        }, () -> {
            sendToStream(QueryHandlerOutbound.newBuilder()
                    .setQueryComplete(QueryComplete.newBuilder()
                            .setRequestId(query.getMessageIdentifier())
                            .build())
                    .build());
        });
    }

    private void handleSubscriptionQuery(SubscriptionQueryRequest request) {
        if (queryHandler == null || !request.hasSubscribe()) {
            return;
        }

        SubscriptionQuery sub = request.getSubscribe();
        String subscriptionId = sub.getSubscriptionIdentifier();

        // 1. Register for updates — the handler will push SubscriptionQueryResponse
        //    messages back through the handlerStream when projections emit updates.
        UpdateSender updateSender = new UpdateSender(subscriptionId);
        queryHandler.registerSubscriptionQuery(sub, updateSender);

        // 2. Handle the initial query — send the result back as a regular query response.
        //    KronosDB server will forward this as the initial_result to the subscriber.
        if (sub.hasQueryRequest()) {
            handleIncomingQuery(sub.getQueryRequest(), "");
        }
    }

    /**
     * Sends subscription query updates back through the handler's OpenStream.
     */
    private class UpdateSender implements SubscriptionUpdateSender {

        private final String subscriptionId;

        UpdateSender(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }

        @Override
        public void sendUpdate(SubscriptionQueryResponse response) {
            sendToStream(QueryHandlerOutbound.newBuilder()
                    .setSubscriptionQueryResponse(response)
                    .build());
        }

        @Override
        public void complete() {
            sendToStream(QueryHandlerOutbound.newBuilder()
                    .setSubscriptionQueryResponse(SubscriptionQueryResponse.newBuilder()
                            .setSubscriptionIdentifier(subscriptionId)
                            .setComplete(QueryUpdateComplete.newBuilder().build())
                            .build())
                    .build());
        }
    }

    /**
     * Prepares for disconnect by completing the handler stream.
     */
    public void prepareDisconnect() {
        if (handlerStream != null) {
            handlerStream.onCompleted();
            handlerStream = null;
        }
    }

    /**
     * Callback interface for handling incoming queries.
     */
    public interface QueryHandler {
        void handle(QueryRequest query,
                    java.util.function.Consumer<QueryResponse> responseSender,
                    Runnable onComplete);

        /**
         * Registers a subscription query. The handler should call
         * {@code incomingHandler.registerUpdateHandler()} and bridge updates
         * back through the {@link SubscriptionUpdateSender}.
         */
        Registration registerSubscriptionQuery(SubscriptionQuery query, SubscriptionUpdateSender updateSender);
    }

    /**
     * Interface for sending subscription query updates back to KronosDB.
     */
    public interface SubscriptionUpdateSender {
        void sendUpdate(SubscriptionQueryResponse response);
        void complete();
    }

    /**
     * Result of a subscription query, providing both a result stream and the ability
     * to send flow control / unsubscribe messages.
     */
    public record SubscriptionQueryResult(
            ResultStream<SubscriptionQueryResponse> results,
            StreamObserver<SubscriptionQueryRequest> requestStream,
            String subscriptionId
    ) {
        public void close() {
            try {
                requestStream.onNext(SubscriptionQueryRequest.newBuilder()
                        .setUnsubscribe(SubscriptionQuery.newBuilder()
                                .setSubscriptionIdentifier(subscriptionId)
                                .build())
                        .build());
                requestStream.onCompleted();
            } catch (IllegalStateException e) {
                // Stream already completed (server closed or errored) — safe to ignore.
            }
        }
    }
}
