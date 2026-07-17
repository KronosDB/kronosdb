package io.kronosdb.connector.axon.event;

import io.kronosdb.connector.grpc.EventStoreChannel;
import io.kronosdb.connector.grpc.KronosDbConnection;
import io.kronosdb.connector.grpc.ResultStream;
import io.kronosdb.grpc.eventstore.AppendRequest;
import io.kronosdb.grpc.eventstore.AppendResponse;
import io.kronosdb.grpc.eventstore.SourceRequest;
import io.kronosdb.grpc.eventstore.SourceResponse;
import io.kronosdb.grpc.eventstore.StreamResponse;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EmptyAppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link EventStorageEngine} implementation backed by KronosDB.
 * <p>
 * Delegates all event store operations to a KronosDB instance via gRPC.
 */
public class KronosDbEventStorageEngine implements EventStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(KronosDbEventStorageEngine.class);

    private final KronosDbConnection connection;
    private final TaggedEventConverter converter;

    /**
     * Constructs a {@code KronosDbEventStorageEngine}.
     *
     * @param connection the KronosDB connection to use
     * @param converter  the event converter for serializing/deserializing payloads
     */
    public KronosDbEventStorageEngine(KronosDbConnection connection, EventConverter converter) {
        this.connection = Objects.requireNonNull(connection, "The KronosDB connection cannot be null.");
        this.converter = new TaggedEventConverter(converter);
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                 @Nullable ProcessingContext context,
                                                                 List<TaggedEventMessage<?>> events) {
        if (events.isEmpty()) {
            return CompletableFuture.completedFuture(EmptyAppendTransaction.INSTANCE);
        }

        EventStoreChannel.AppendTransaction appendTransaction = eventChannel().startAppendTransaction();

        AppendRequest.Builder requestBuilder = AppendRequest.newBuilder()
                .setCondition(ConditionConverter.convertAppendCondition(condition));

        events.stream()
                .map(converter::convertTaggedEventMessage)
                .forEach(taggedEvent -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Appending event [{}] with timestamp [{}], tags: {}.",
                                taggedEvent.getEvent().getIdentifier(),
                                taggedEvent.getEvent().getTimestamp(),
                                taggedEvent.getTagsList().stream()
                                        .map(t -> t.getKey().toStringUtf8() + "=" + t.getValue().toStringUtf8())
                                        .toList());
                    }
                    requestBuilder.addEvents(taggedEvent);
                });

        appendTransaction.append(requestBuilder.build());

        return CompletableFuture.completedFuture(new KronosDbAppendTransaction(appendTransaction));
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition) {
        if (logger.isDebugEnabled()) {
            logger.debug("Sourcing events with condition [{}].", condition);
        }

        SourceRequest request = ConditionConverter.convertSourcingCondition(condition);
        if (logger.isDebugEnabled()) {
            logger.debug("Source request: from={}, criteria={}.",
                    request.getFromSequence(),
                    request.getCriteriaList().stream()
                            .map(c -> "names=" + c.getNamesList() + " tags=" + c.getTagsList().stream()
                                    .map(t -> t.getKey().toStringUtf8() + "=" + t.getValue().toStringUtf8())
                                    .toList())
                            .toList());
        }
        ResultStream<SourceResponse> stream = eventChannel().source(request);
        return new SourcingEventMessageStream(stream, converter);
    }

    @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        if (logger.isDebugEnabled()) {
            logger.debug("Streaming events with condition [{}].", condition);
        }

        long fromSequence = ConditionConverter.extractStreamFromSequence(condition);
        var criteria = ConditionConverter.extractStreamCriteria(condition);
        ResultStream<StreamResponse> stream = eventChannel().stream(fromSequence, criteria);
        return new StreamingEventMessageStream(stream, converter);
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        logger.debug("Retrieving first token (tail).");
        return eventChannel().tail()
                .thenApply(response -> new GlobalSequenceTrackingToken(response.getSequence()));
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        logger.debug("Retrieving latest token (head).");
        return eventChannel().head()
                .thenApply(response -> new GlobalSequenceTrackingToken(response.getSequence()));
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        logger.debug("Retrieving token at timestamp [{}].", at);
        return eventChannel().getSequenceAt(at.toEpochMilli())
                .thenApply(response -> new GlobalSequenceTrackingToken(
                        response.getSequence() == -1 ? 0 : response.getSequence()));
    }

    private EventStoreChannel eventChannel() {
        return connection.eventStoreChannel();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("connection", connection);
        descriptor.describeProperty("converter", converter);
    }

    private record KronosDbAppendTransaction(
            EventStoreChannel.AppendTransaction appendTransaction
    ) implements AppendTransaction<AppendResponse> {

        @Override
        public CompletableFuture<AppendResponse> commit() {
            logger.debug("Committing append transaction...");
            return appendTransaction.commit()
                    .exceptionallyCompose(throwable -> {
                        logger.warn("Append transaction commit failed.", throwable);
                        return CompletableFuture.failedFuture(
                                new AppendEventsTransactionRejectedException(throwable.getMessage())
                        );
                    });
        }

        @Override
        public CompletableFuture<ConsistencyMarker> afterCommit(AppendResponse appendResponse) {
            long marker = appendResponse.getConsistencyMarker();
            logger.debug("Append transaction succeeded with marker [{}].", marker);
            return CompletableFuture.completedFuture(new GlobalIndexConsistencyMarker(marker));
        }

        @Override
        public void rollback() {
            appendTransaction.rollback();
        }
    }
}
