package io.kronosdb.connector.axon.event;

import io.kronosdb.grpc.platform.EventProcessorInfo;
import io.kronosdb.grpc.platform.EventProcessorInfo.SegmentStatus;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.EventTrackerStatus;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessor;

import java.util.List;

/**
 * Utility class constructing {@link EventProcessorInfo} instances for all known
 * {@link EventProcessor} types.
 * <p>
 * Mirrors the Axon Server connector's {@code EventProcessorInfoUtils} so that
 * KronosDB receives the same processor status shape.
 */
final class EventProcessorInfoUtils {

    private static final String POOLED_STREAMING = "Pooled Streaming";
    private static final String SUBSCRIBING = "Subscribing";
    private static final String UNKNOWN = "Unknown";

    static EventProcessorInfo describeStreaming(StreamingEventProcessor processor) {
        List<SegmentStatus> segmentStatuses = processor.processingStatus()
                .values()
                .stream()
                .map(EventProcessorInfoUtils::buildSegmentStatus)
                .toList();

        return EventProcessorInfo.newBuilder()
                .setProcessorName(processor.name())
                .setTokenStoreIdentifier(processor.getTokenStoreIdentifier())
                .setMode(POOLED_STREAMING)
                .setActiveThreads(processor.processingStatus().size())
                .setAvailableThreads(
                        processor.maxCapacity() - processor.processingStatus().size()
                )
                .setRunning(processor.isRunning())
                .setError(processor.isError())
                .addAllSegmentStatus(segmentStatuses)
                .setIsStreamingProcessor(true)
                .build();
    }

    static EventProcessorInfo describeSubscribing(SubscribingEventProcessor processor) {
        return EventProcessorInfo.newBuilder()
                .setProcessorName(processor.name())
                .setMode(SUBSCRIBING)
                .setIsStreamingProcessor(false)
                .build();
    }

    static EventProcessorInfo describeUnknown(EventProcessor processor) {
        return EventProcessorInfo.newBuilder()
                .setProcessorName(processor.name())
                .setMode(UNKNOWN)
                .setIsStreamingProcessor(false)
                .build();
    }

    private static SegmentStatus buildSegmentStatus(EventTrackerStatus status) {
        return SegmentStatus.newBuilder()
                .setSegmentId(status.getSegment().getSegmentId())
                .setCaughtUp(status.isCaughtUp())
                .setReplaying(status.isReplaying())
                .setOnePartOf(status.getSegment().getMask() + 1)
                .setTokenPosition(getPosition(status.getTrackingToken()))
                .setErrorState(status.isErrorState() ? buildErrorMessage(status.getError()) : "")
                .build();
    }

    private static long getPosition(TrackingToken trackingToken) {
        if (trackingToken != null) {
            return trackingToken.position().orElse(0);
        }
        return 0;
    }

    private static String buildErrorMessage(Throwable error) {
        return error.getClass().getName() + ": " + error.getMessage();
    }

    private EventProcessorInfoUtils() {
    }
}
