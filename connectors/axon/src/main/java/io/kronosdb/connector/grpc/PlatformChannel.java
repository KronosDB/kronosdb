package io.kronosdb.connector.grpc;

import io.grpc.stub.StreamObserver;
import io.kronosdb.grpc.ErrorMessage;
import io.kronosdb.grpc.InstructionAck;
import io.kronosdb.grpc.platform.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Channel abstraction for KronosDB PlatformService gRPC operations.
 * Manages the client lifecycle, heartbeat, event processor reporting,
 * and handling of server-initiated processor instructions.
 */
public class PlatformChannel {

    private static final Logger logger = LoggerFactory.getLogger(PlatformChannel.class);

    // Reconnection backoff constants
    private static final long BACKOFF_INITIAL_MS = 500;
    private static final long BACKOFF_MAX_MS = 30_000;
    private static final double BACKOFF_JITTER_FACTOR = 0.25;

    private final PlatformServiceGrpc.PlatformServiceStub asyncStub;
    private final PlatformServiceGrpc.PlatformServiceBlockingStub blockingStub;
    private final PlatformServiceGrpc.PlatformServiceFutureStub futureStub;
    private final String context;

    private volatile StreamObserver<PlatformInbound> platformStream;
    private ScheduledExecutorService heartbeatExecutor;
    private volatile ClientIdentification lastIdentification;
    private volatile Runnable lastReconnectCallback;
    private volatile boolean reconnecting = false;
    private volatile boolean disconnected = false;

    /**
     * Handler for server-initiated event processor instructions.
     * <p>
     * Set via {@link #setInstructionHandler(InstructionHandler)} before opening the stream.
     * When the server sends a processor instruction (pause, start, split, merge, release),
     * the handler is invoked. The handler should return a {@link CompletableFuture} that
     * completes when the instruction has been executed, with {@code true} for success.
     */
    private volatile InstructionHandler instructionHandler;

    PlatformChannel(PlatformServiceGrpc.PlatformServiceStub asyncStub,
                    PlatformServiceGrpc.PlatformServiceBlockingStub blockingStub,
                    PlatformServiceGrpc.PlatformServiceFutureStub futureStub,
                    String context) {
        this.asyncStub = asyncStub;
        this.blockingStub = blockingStub;
        this.futureStub = futureStub;
        this.context = context;
    }

    /**
     * Connects to the platform service by identifying this client.
     */
    public CompletableFuture<PlatformInfo> connect(ClientIdentification identification) {
        return GrpcFutures.toCompletableFuture(futureStub.getPlatformServer(identification));
    }

    /**
     * Sets the handler for server-initiated event processor instructions.
     */
    public void setInstructionHandler(InstructionHandler handler) {
        this.instructionHandler = handler;
    }

    /**
     * Opens the persistent lifecycle stream for heartbeat, processor reporting,
     * and server instructions.
     */
    public void openStream(ClientIdentification identification, Runnable onReconnectRequested) {
        this.lastIdentification = identification;
        this.lastReconnectCallback = onReconnectRequested;
        openStreamInternal(identification, onReconnectRequested);
    }

    private void openStreamInternal(ClientIdentification identification, Runnable onReconnectRequested) {
        platformStream = asyncStub.openStream(new StreamObserver<>() {
            @Override
            public void onNext(PlatformOutbound outbound) {
                if (outbound.hasHeartbeat()) {
                    sendHeartbeat();
                } else if (outbound.hasRequestReconnect()) {
                    logger.info("Server requested reconnect for context [{}].", context);
                    if (onReconnectRequested != null) {
                        onReconnectRequested.run();
                    }
                    // Acknowledge reconnect instruction
                    if (!outbound.getInstructionId().isEmpty()) {
                        sendAck(outbound.getInstructionId(), true, null);
                    }
                    return;
                } else if (outbound.hasPauseEventProcessor()) {
                    handleProcessorInstruction(outbound.getInstructionId(),
                            ProcessorInstruction.pause(outbound.getPauseEventProcessor().getProcessorName()));
                    return;
                } else if (outbound.hasStartEventProcessor()) {
                    handleProcessorInstruction(outbound.getInstructionId(),
                            ProcessorInstruction.start(outbound.getStartEventProcessor().getProcessorName()));
                    return;
                } else if (outbound.hasSplitEventProcessorSegment()) {
                    var ref = outbound.getSplitEventProcessorSegment();
                    handleProcessorInstruction(outbound.getInstructionId(),
                            ProcessorInstruction.split(ref.getProcessorName(), ref.getSegmentIdentifier()));
                    return;
                } else if (outbound.hasMergeEventProcessorSegment()) {
                    var ref = outbound.getMergeEventProcessorSegment();
                    handleProcessorInstruction(outbound.getInstructionId(),
                            ProcessorInstruction.merge(ref.getProcessorName(), ref.getSegmentIdentifier()));
                    return;
                } else if (outbound.hasReleaseSegment()) {
                    var ref = outbound.getReleaseSegment();
                    handleProcessorInstruction(outbound.getInstructionId(),
                            ProcessorInstruction.releaseSegment(ref.getProcessorName(), ref.getSegmentIdentifier()));
                    return;
                } else if (outbound.hasRequestEventProcessorInfo()) {
                    handleProcessorInstruction(outbound.getInstructionId(),
                            ProcessorInstruction.requestInfo(outbound.getRequestEventProcessorInfo().getProcessorName()));
                    return;
                }

                // Generic ack for unknown instructions
                if (!outbound.getInstructionId().isEmpty()) {
                    sendAck(outbound.getInstructionId(), true, null);
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Platform stream error for context [{}]: {}.", context, t.getMessage());
                platformStream = null;
                schedulePlatformReconnect();
            }

            @Override
            public void onCompleted() {
                logger.info("Platform stream completed for context [{}].", context);
                platformStream = null;
                schedulePlatformReconnect();
            }
        });

        // Send initial registration
        platformStream.onNext(PlatformInbound.newBuilder()
                .setRegister(identification)
                .build());
    }

    /**
     * Sends an event processor info report to the server.
     */
    public void sendEventProcessorInfo(EventProcessorInfo info) {
        if (platformStream != null) {
            platformStream.onNext(PlatformInbound.newBuilder()
                    .setEventProcessorInfo(info)
                    .build());
        }
    }

    /**
     * Enables periodic heartbeat messages.
     */
    public void enableHeartbeat(long intervalMillis, long timeoutMillis) {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kronosdb-heartbeat-" + context);
            t.setDaemon(true);
            return t;
        });
        heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeat, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat() {
        if (platformStream != null) {
            platformStream.onNext(PlatformInbound.newBuilder()
                    .setHeartbeat(Heartbeat.getDefaultInstance())
                    .build());
        }
    }

    private void sendAck(String instructionId, boolean success, String errorMessage) {
        if (platformStream != null) {
            var ackBuilder = InstructionAck.newBuilder()
                    .setInstructionId(instructionId)
                    .setSuccess(success);
            if (errorMessage != null) {
                ackBuilder.setError(ErrorMessage.newBuilder().setMessage(errorMessage).build());
            }
            platformStream.onNext(PlatformInbound.newBuilder()
                    .setAck(ackBuilder.build())
                    .build());
        }
    }

    private void sendInstructionResult(String instructionId, boolean success, String errorMessage) {
        if (platformStream != null) {
            var resultBuilder = InstructionResult.newBuilder()
                    .setInstructionId(instructionId)
                    .setSuccess(success);
            if (errorMessage != null) {
                resultBuilder.setError(ErrorMessage.newBuilder().setMessage(errorMessage).build());
            }
            platformStream.onNext(PlatformInbound.newBuilder()
                    .setResult(resultBuilder.build())
                    .build());
        }
    }

    private void handleProcessorInstruction(String instructionId, ProcessorInstruction instruction) {
        InstructionHandler handler = this.instructionHandler;
        if (handler == null) {
            logger.debug("No instruction handler set, ignoring {} instruction for processor [{}].",
                    instruction.type(), instruction.processorName());
            if (!instructionId.isEmpty()) {
                sendAck(instructionId, true, null);
            }
            return;
        }

        logger.debug("Received {} instruction for processor [{}].", instruction.type(), instruction.processorName());

        // Acknowledge that we received it
        if (!instructionId.isEmpty()) {
            sendAck(instructionId, true, null);
        }

        // Execute the instruction asynchronously and send the result
        handler.handle(instruction).whenComplete((success, error) -> {
            if (!instructionId.isEmpty()) {
                if (error != null) {
                    sendInstructionResult(instructionId, false, error.getMessage());
                } else {
                    sendInstructionResult(instructionId, success != null && success, null);
                }
            }
        });
    }

    private void schedulePlatformReconnect() {
        if (disconnected || lastIdentification == null) {
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
                while (!disconnected) {
                    attempt++;
                    long delay = calculateBackoff(attempt);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    try {
                        logger.info("Reconnecting platform stream for context [{}] (attempt {}, delay {}ms).",
                                context, attempt, delay);
                        openStreamInternal(lastIdentification, lastReconnectCallback);

                        if (platformStream == null) {
                            logger.warn("Platform stream open returned but stream is null — retrying.");
                            continue;
                        }

                        logger.info("Platform stream reconnected for context [{}].", context);
                        return;
                    } catch (Exception e) {
                        logger.warn("Platform reconnect attempt {} failed for context [{}]: {}.",
                                attempt, context, e.getMessage());
                        platformStream = null;
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

    /**
     * Disconnects the platform stream.
     */
    public void disconnect() {
        disconnected = true;
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        if (platformStream != null) {
            platformStream.onCompleted();
            platformStream = null;
        }
    }

    /**
     * Handler interface for server-initiated event processor instructions.
     */
    @FunctionalInterface
    public interface InstructionHandler {
        /**
         * Handles a processor instruction from the server.
         *
         * @param instruction the instruction to execute
         * @return a future that completes with {@code true} on success, {@code false} on failure
         */
        CompletableFuture<Boolean> handle(ProcessorInstruction instruction);
    }

    /**
     * Represents a server instruction for an event processor.
     */
    public record ProcessorInstruction(Type type, String processorName, int segmentId) {

        public enum Type {
            PAUSE, START, SPLIT, MERGE, RELEASE_SEGMENT, REQUEST_INFO
        }

        static ProcessorInstruction pause(String processorName) {
            return new ProcessorInstruction(Type.PAUSE, processorName, -1);
        }

        static ProcessorInstruction start(String processorName) {
            return new ProcessorInstruction(Type.START, processorName, -1);
        }

        static ProcessorInstruction split(String processorName, int segmentId) {
            return new ProcessorInstruction(Type.SPLIT, processorName, segmentId);
        }

        static ProcessorInstruction merge(String processorName, int segmentId) {
            return new ProcessorInstruction(Type.MERGE, processorName, segmentId);
        }

        static ProcessorInstruction releaseSegment(String processorName, int segmentId) {
            return new ProcessorInstruction(Type.RELEASE_SEGMENT, processorName, segmentId);
        }

        static ProcessorInstruction requestInfo(String processorName) {
            return new ProcessorInstruction(Type.REQUEST_INFO, processorName, -1);
        }
    }
}
