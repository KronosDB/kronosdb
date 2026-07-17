package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.grpc.ResultStream;
import io.kronosdb.grpc.eventstore.SequencedEvent;
import io.kronosdb.grpc.eventstore.StreamResponse;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link MessageStream} backed by a KronosDB stream (infinite live-tailing) event stream.
 * Translates KronosDB {@link StreamResponse} messages to Axon Framework {@link EventMessage}s.
 * <p>
 * The server sends events in batches ({@code SequencedEventBatch}); this stream unpacks
 * each batch into an internal buffer and hands events out one at a time. Heartbeat
 * frames are consumed and skipped transparently.
 * <p>
 * This stream remains open and pushes new events as they are appended to KronosDB.
 */
public class StreamingEventMessageStream implements MessageStream<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(StreamingEventMessageStream.class);

    private final ResultStream<StreamResponse> stream;
    private final TaggedEventConverter converter;

    /** Events from the current batch not yet handed out. */
    private final Deque<SequencedEvent> buffer = new ArrayDeque<>();

    public StreamingEventMessageStream(ResultStream<StreamResponse> stream, TaggedEventConverter converter) {
        this.stream = Objects.requireNonNull(stream);
        this.converter = Objects.requireNonNull(converter);
    }

    /**
     * Pulls responses (skipping heartbeats) until the buffer has an event or
     * no message is available without blocking.
     */
    private void fill() {
        while (buffer.isEmpty()) {
            StreamResponse response = stream.nextIfAvailable();
            if (response == null) {
                return;
            }
            if (response.hasBatch()) {
                buffer.addAll(response.getBatch().getEventsList());
            } else {
                logger.trace("Received stream heartbeat, skipping.");
            }
        }
    }

    @Override
    public Optional<Entry<EventMessage>> next() {
        fill();
        SequencedEvent event = buffer.pollFirst();
        return event == null ? Optional.empty() : Optional.of(convertToEntry(event));
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        fill();
        SequencedEvent event = buffer.peekFirst();
        return event == null ? Optional.empty() : Optional.of(convertToEntry(event));
    }

    private SimpleEntry<EventMessage> convertToEntry(SequencedEvent event) {
        EventMessage eventMessage = converter.convertEvent(event.getEvent());
        TrackingToken token = new GlobalSequenceTrackingToken(event.getSequence() + 1);
        Context context = Context.with(TrackingToken.RESOURCE_KEY, token);
        return new SimpleEntry<>(eventMessage, context);
    }

    @Override
    public void setCallback(Runnable callback) {
        stream.onAvailable(callback);
    }

    @Override
    public Optional<Throwable> error() {
        return stream.getError();
    }

    @Override
    public boolean isCompleted() {
        return stream.isClosed() && buffer.isEmpty();
    }

    @Override
    public boolean hasNextAvailable() {
        fill();
        return !buffer.isEmpty();
    }

    @Override
    public void close() {
        stream.close();
    }
}
