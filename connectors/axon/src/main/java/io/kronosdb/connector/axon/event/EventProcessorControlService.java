package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.grpc.PlatformChannel;
import io.kronosdb.connector.grpc.PlatformChannel.ProcessorInstruction;
import io.kronosdb.grpc.platform.EventProcessorInfo;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Service that reports event processor status to KronosDB and handles
 * server-initiated processor instructions (pause, start, split, merge, release).
 * <p>
 * Mirrors the Axon Server connector's {@code EventProcessorControlService}.
 * On {@link #start()}, it registers all event processors with the platform channel,
 * sets up periodic status reporting, and installs an instruction handler that
 * delegates to the appropriate processor.
 */
public class EventProcessorControlService {

    private static final Logger logger = LoggerFactory.getLogger(EventProcessorControlService.class);
    private static final long REPORT_INTERVAL_MS = 5000;

    private final Configuration configuration;
    private final PlatformChannel platformChannel;
    private ScheduledExecutorService reportExecutor;

    public EventProcessorControlService(Configuration configuration, PlatformChannel platformChannel) {
        this.configuration = configuration;
        this.platformChannel = platformChannel;
    }

    /**
     * Registers all event processors for status reporting and installs the
     * instruction handler on the platform channel.
     */
    public void start() {
        Map<String, EventProcessor> processors = configuration.getComponents(EventProcessor.class);
        if (processors.isEmpty()) {
            logger.debug("No event processors found, skipping processor control registration.");
            return;
        }

        logger.info("Registering {} event processor(s) with KronosDB.", processors.size());

        // Install instruction handler that routes to the right processor.
        platformChannel.setInstructionHandler(instruction -> handleInstruction(processors, instruction));

        // Schedule periodic status reporting.
        reportExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kronosdb-processor-report");
            t.setDaemon(true);
            return t;
        });
        reportExecutor.scheduleAtFixedRate(
                () -> reportAll(processors),
                0, REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stops periodic reporting.
     */
    public void shutdown() {
        if (reportExecutor != null) {
            reportExecutor.shutdown();
        }
    }

    private void reportAll(Map<String, EventProcessor> processors) {
        for (EventProcessor processor : processors.values()) {
            try {
                EventProcessorInfo info = describeProcessor(processor);
                platformChannel.sendEventProcessorInfo(info);
            } catch (Exception e) {
                logger.debug("Failed to report processor [{}] status.", processor.name(), e);
            }
        }
    }

    private EventProcessorInfo describeProcessor(EventProcessor processor) {
        if (processor instanceof StreamingEventProcessor streaming) {
            return EventProcessorInfoUtils.describeStreaming(streaming);
        } else if (processor instanceof SubscribingEventProcessor subscribing) {
            return EventProcessorInfoUtils.describeSubscribing(subscribing);
        } else {
            return EventProcessorInfoUtils.describeUnknown(processor);
        }
    }

    private CompletableFuture<Boolean> handleInstruction(
            Map<String, EventProcessor> processors,
            ProcessorInstruction instruction
    ) {
        EventProcessor processor = processors.get(instruction.processorName());
        if (processor == null) {
            logger.warn("Received {} instruction for unknown processor [{}].",
                    instruction.type(), instruction.processorName());
            return CompletableFuture.completedFuture(false);
        }

        return switch (instruction.type()) {
            case PAUSE -> processor.shutdown().thenApply(v -> true);
            case START -> processor.start().thenApply(v -> true);
            case SPLIT -> handleStreamingOp(processor, instruction,
                    sep -> sep.splitSegment(instruction.segmentId()));
            case MERGE -> handleStreamingOp(processor, instruction,
                    sep -> sep.mergeSegment(instruction.segmentId()));
            case RELEASE_SEGMENT -> {
                if (processor instanceof StreamingEventProcessor sep) {
                    sep.releaseSegment(instruction.segmentId());
                    yield CompletableFuture.completedFuture(true);
                } else {
                    logger.info("Release segment requested for non-streaming processor [{}].",
                            instruction.processorName());
                    yield CompletableFuture.completedFuture(false);
                }
            }
            case REQUEST_INFO -> {
                try {
                    platformChannel.sendEventProcessorInfo(describeProcessor(processor));
                } catch (Exception e) {
                    logger.debug("Failed to send requested processor info for [{}].",
                            instruction.processorName(), e);
                }
                yield CompletableFuture.completedFuture(true);
            }
        };
    }

    private CompletableFuture<Boolean> handleStreamingOp(
            EventProcessor processor,
            ProcessorInstruction instruction,
            java.util.function.Function<StreamingEventProcessor, CompletableFuture<Boolean>> operation
    ) {
        if (!(processor instanceof StreamingEventProcessor sep)) {
            logger.info("{} requested for non-streaming processor [{}].",
                    instruction.type(), instruction.processorName());
            return CompletableFuture.completedFuture(false);
        }
        return operation.apply(sep).thenApply(result -> {
            if (Boolean.TRUE.equals(result)) {
                logger.info("Successfully executed {} on segment [{}] of processor [{}].",
                        instruction.type(), instruction.segmentId(), instruction.processorName());
            } else {
                logger.warn("Failed to execute {} on segment [{}] of processor [{}].",
                        instruction.type(), instruction.segmentId(), instruction.processorName());
            }
            return result;
        });
    }
}
