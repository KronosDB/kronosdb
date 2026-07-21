package io.kronosdb.connector.grpc;

import io.grpc.stub.StreamObserver;
import io.kronosdb.grpc.FlowControl;
import io.kronosdb.grpc.InstructionAck;
import io.kronosdb.grpc.command.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel abstraction for KronosDB CommandService gRPC operations.
 * Manages the bidirectional stream for command handler registration and
 * provides dispatch capability.
 */
public class CommandChannel {

    private static final Logger logger = LoggerFactory.getLogger(CommandChannel.class);

    // Reconnection backoff constants
    private static final long BACKOFF_INITIAL_MS = 500;
    private static final long BACKOFF_MAX_MS = 30_000;
    private static final double BACKOFF_JITTER_FACTOR = 0.25;

    // Flow control constants
    private static final long INITIAL_PERMITS = 500;
    private static final double REFILL_THRESHOLD = 0.25;

    private final CommandServiceGrpc.CommandServiceStub asyncStub;
    private final CommandServiceGrpc.CommandServiceFutureStub futureStub;
    private final String context;

    private volatile StreamObserver<CommandHandlerOutbound> handlerStream;
    private final Object streamWriteLock = new Object();
    private final Map<String, CompletableFuture<InstructionAck>> pendingAcks = new ConcurrentHashMap<>();
    private Function<Command, CompletableFuture<CommandResponse>> commandHandler;

    // Tracked state for reconnection: everything needed to re-register after stream drop.
    private volatile String registeredClientId;
    private volatile String registeredComponentName;
    private volatile int registeredLoadFactor;
    private final Map<String, CommandSubscription> activeSubscriptions = new ConcurrentHashMap<>();
    private volatile boolean reconnecting = false;

    // Adaptive flow control: track how many permits have been consumed.
    private final AtomicLong permitsRemaining = new AtomicLong(0);
    private volatile long currentPermitBatch = INITIAL_PERMITS;

    CommandChannel(CommandServiceGrpc.CommandServiceStub asyncStub,
                   CommandServiceGrpc.CommandServiceFutureStub futureStub,
                   String context) {
        this.asyncStub = asyncStub;
        this.futureStub = futureStub;
        this.context = context;
    }

    /**
     * Dispatches a command to a registered handler and returns the result.
     */
    public CompletableFuture<CommandResponse> sendCommand(Command command) {
        return GrpcFutures.toCompletableFuture(futureStub.dispatch(command));
    }

    /**
     * Registers a command handler for the given command name.
     * Opens a bidirectional stream if not already open.
     *
     * @param handler    function that processes incoming commands
     * @param loadFactor relative load factor for load balancing
     * @param commandNames the command type names this handler can process
     * @return a registration that can be cancelled to unsubscribe
     */
    public Registration registerCommandHandler(
            Function<Command, CompletableFuture<CommandResponse>> handler,
            int loadFactor,
            String clientId,
            String componentName,
            String... commandNames) {
        this.commandHandler = handler;
        this.registeredClientId = clientId;
        this.registeredComponentName = componentName;
        this.registeredLoadFactor = loadFactor;

        ensureHandlerStreamOpen();

        // Collect ack futures for all subscriptions (wait in parallel, not sequentially)
        List<CompletableFuture<?>> ackFutures = new ArrayList<>();

        for (String commandName : commandNames) {
            CommandSubscription subscription = CommandSubscription.newBuilder()
                    .setMessageId(UUID.randomUUID().toString())
                    .setCommand(commandName)
                    .setLoadFactor(loadFactor)
                    .setClientId(clientId)
                    .setComponentName(componentName)
                    .build();

            activeSubscriptions.put(commandName, subscription);
            CompletableFuture<InstructionAck> ackFuture = sendSubscription(subscription);
            if (ackFuture != null) {
                ackFutures.add(ackFuture);
            }
        }

        // Grant initial permits
        grantPermits(clientId, INITIAL_PERMITS);
        permitsRemaining.set(INITIAL_PERMITS);
        currentPermitBatch = INITIAL_PERMITS;

        // All acks resolve in parallel — worst case is one 5s timeout, not N * 5s
        CompletableFuture<Void> registrationAck = CompletableFuture.allOf(
                ackFutures.toArray(new CompletableFuture[0]));

        return new Registration(() -> {
            for (String commandName : commandNames) {
                activeSubscriptions.remove(commandName);
            }
            for (String commandName : commandNames) {
                sendToStream(CommandHandlerOutbound.newBuilder()
                        .setUnsubscribe(CommandSubscription.newBuilder()
                                .setCommand(commandName)
                                .build())
                        .build());
            }
        }, registrationAck);
    }

    private void sendToStream(CommandHandlerOutbound message) {
        StreamObserver<CommandHandlerOutbound> stream = handlerStream;
        if (stream != null) {
            synchronized (streamWriteLock) {
                stream.onNext(message);
            }
        }
    }

    private CompletableFuture<InstructionAck> sendSubscription(CommandSubscription subscription) {
        if (handlerStream != null) {
            String instructionId = UUID.randomUUID().toString();
            CompletableFuture<InstructionAck> ackFuture = new CompletableFuture<>();
            pendingAcks.put(instructionId, ackFuture);

            sendToStream(CommandHandlerOutbound.newBuilder()
                    .setSubscribe(subscription)
                    .setInstructionId(instructionId)
                    .build());

            // Time out the ack after 5 seconds so we don't block forever; the timeout
            // is converted into a synthetic success ack so registration proceeds.
            return ackFuture.orTimeout(5, TimeUnit.SECONDS)
                    .exceptionally(ex -> {
                        pendingAcks.remove(instructionId);
                        logger.debug("Ack timeout for command subscription [{}] instruction [{}].",
                                subscription.getCommand(), instructionId);
                        return InstructionAck.newBuilder()
                                .setInstructionId(instructionId)
                                .setSuccess(true)
                                .build();
                    });
        }
        return null;
    }

    private void grantPermits(String clientId, long permits) {
        sendToStream(CommandHandlerOutbound.newBuilder()
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
            logger.debug("Refilled {} command permits for context [{}]. Remaining: {}.",
                    refill, context, permitsRemaining.get());
        }
    }

    private synchronized void ensureHandlerStreamOpen() {
        if (handlerStream != null) {
            return;
        }

        handlerStream = asyncStub.openStream(new StreamObserver<>() {
            @Override
            public void onNext(CommandHandlerInbound inbound) {

                if (inbound.hasCommand()) {
                    permitsRemaining.decrementAndGet();
                    handleIncomingCommand(inbound.getCommand(), inbound.getInstructionId());
                    maybeRefillPermits();
                } else if (inbound.hasAck()) {
                    handleAck(inbound.getAck());
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Command handler stream error for context [{}]: {}.", context, t.getMessage());
                handlerStream = null;
                scheduleReconnect();
            }

            @Override
            public void onCompleted() {
                logger.info("Command handler stream completed for context [{}].", context);
                handlerStream = null;
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (activeSubscriptions.isEmpty()) {
            return;
        }
        // Use compareAndSet pattern — if already reconnecting, don't start another loop.
        // But if the previous reconnect "succeeded" and then the stream died again,
        // reconnecting will be false and we MUST start a new loop.
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
                        logger.info("Reconnecting command handler stream for context [{}] (attempt {}, delay {}ms).",
                                context, attempt, delay);
                        ensureHandlerStreamOpen();

                        if (handlerStream == null) {
                            logger.warn("Stream open returned but handlerStream is null — retrying.");
                            continue;
                        }

                        // Re-register all active subscriptions.
                        for (CommandSubscription sub : activeSubscriptions.values()) {
                            sendSubscription(sub);
                        }
                        if (registeredClientId != null) {
                            grantPermits(registeredClientId, INITIAL_PERMITS);
                            permitsRemaining.set(INITIAL_PERMITS);
                        }

                        // Verify the stream survived registration — if onError fired
                        // during re-registration, handlerStream will be null again.
                        if (handlerStream == null) {
                            logger.warn("Stream died during re-registration for context [{}] — retrying.", context);
                            continue;
                        }

                        logger.info("Command handler stream reconnected for context [{}] with {} subscriptions.",
                                context, activeSubscriptions.size());
                        return;
                    } catch (Exception e) {
                        logger.warn("Command handler reconnect attempt {} failed for context [{}]: {}.",
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

    private void handleIncomingCommand(Command command, String instructionId) {
        if (commandHandler == null) {
            logger.warn("Received command [{}] but no handler is registered.", command.getName());
            return;
        }

        commandHandler.apply(command).whenComplete((response, error) -> {
            if (error != null) {
                logger.error("Error handling command [{}].", command.getName(), error);
                response = CommandResponse.newBuilder()
                        .setRequestIdentifier(command.getMessageIdentifier())
                        .setErrorCode("COMMAND_EXECUTION_ERROR")
                        .setErrorMessage(io.kronosdb.grpc.ErrorMessage.newBuilder()
                                .setMessage(error.getMessage())
                                .build())
                        .build();
            }
            sendToStream(CommandHandlerOutbound.newBuilder()
                    .setCommandResponse(response)
                    .build());
        });
    }

    private void handleAck(InstructionAck ack) {
        CompletableFuture<InstructionAck> future = pendingAcks.remove(ack.getInstructionId());
        if (future != null) {
            future.complete(ack);
        }
    }

    /**
     * Prepares for disconnect by completing the handler stream.
     */
    public CompletableFuture<Void> prepareDisconnect() {
        if (handlerStream != null) {
            handlerStream.onCompleted();
            handlerStream = null;
        }
        return CompletableFuture.completedFuture(null);
    }
}
