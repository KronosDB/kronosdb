package io.kronosdb.connector.axon.query;

import io.kronosdb.connector.axon.KronosDbException;
import io.kronosdb.connector.grpc.QueryChannel;
import io.kronosdb.connector.grpc.ResultStream;
import io.kronosdb.grpc.query.SubscriptionQueryResponse;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link MessageStream} that wraps a KronosDB subscription query result,
 * converting {@link SubscriptionQueryResponse} messages (initial results and updates)
 * to Axon Framework {@link QueryResponseMessage}s.
 */
public class SubscriptionQueryResponseMessageStream implements MessageStream<QueryResponseMessage> {

    private final QueryChannel.SubscriptionQueryResult queryResult;
    private final ResultStream<SubscriptionQueryResponse> stream;
    private final @Nullable Converter converter;

    // Terminal state signalled by the handler via complete / complete_exceptionally.
    private volatile boolean terminated;
    private volatile @Nullable Throwable terminalError;

    // Update flow control: refill the server-side credit in batches as updates are consumed.
    private final long refillBatch;
    private final AtomicLong consumedSinceRefill = new AtomicLong(0);

    public SubscriptionQueryResponseMessageStream(QueryChannel.SubscriptionQueryResult queryResult,
                                                   @Nullable Converter converter) {
        this.queryResult = queryResult;
        this.stream = queryResult.results();
        this.converter = converter;
        this.refillBatch = Math.max(1, queryResult.initialPermits() / 4);
    }

    @Override
    public Optional<Entry<QueryResponseMessage>> next() {
        SubscriptionQueryResponse response = stream.nextIfAvailable();
        if (response == null) {
            return Optional.empty();
        }
        Optional<Entry<QueryResponseMessage>> entry = convertResponse(response);
        if (response.hasUpdate()) {
            onUpdateConsumed();
        }
        return entry;
    }

    private void onUpdateConsumed() {
        if (terminated || stream.isClosed()) {
            return;
        }
        if (consumedSinceRefill.incrementAndGet() >= refillBatch) {
            consumedSinceRefill.addAndGet(-refillBatch);
            queryResult.sendFlowControl(refillBatch);
        }
    }

    @Override
    public Optional<Entry<QueryResponseMessage>> peek() {
        SubscriptionQueryResponse response = stream.peek();
        if (response == null) {
            return Optional.empty();
        }
        return convertResponse(response);
    }

    private Optional<Entry<QueryResponseMessage>> convertResponse(SubscriptionQueryResponse response) {
        if (response.hasInitialResult()) {
            return Optional.of(new SimpleEntry<>(
                    QueryConverter.convertQueryResponse(response.getInitialResult(), converter)));
        } else if (response.hasUpdate()) {
            // Convert update payload to a GenericQueryResponseMessage
            var update = response.getUpdate();
            var payload = update.getPayload();
            var message = new GenericMessage(
                    update.getMessageIdentifier(),
                    new MessageType(payload.getType(), payload.getRevision()),
                    payload.getData().toByteArray(),
                    io.kronosdb.connector.axon.MetadataConverter.fromGrpcMetadata(update.getMetadataMap())
            );
            return Optional.of(new SimpleEntry<>(
                    new GenericQueryResponseMessage(message).withConverter(converter)));
        } else if (response.hasComplete()) {
            terminated = true;
        } else if (response.hasCompleteExceptionally()) {
            var completeExceptionally = response.getCompleteExceptionally();
            terminalError = new KronosDbException(
                    completeExceptionally.getErrorCode(),
                    completeExceptionally.getErrorMessage().getMessage());
            terminated = true;
        }
        return Optional.empty();
    }

    @Override
    public void setCallback(Runnable callback) {
        stream.onAvailable(callback);
    }

    @Override
    public Optional<Throwable> error() {
        Throwable error = terminalError;
        if (error != null) {
            return Optional.of(error);
        }
        return stream.getError();
    }

    @Override
    public boolean isCompleted() {
        return terminated || stream.isClosed();
    }

    @Override
    public boolean hasNextAvailable() {
        return !terminated && stream.peek() != null;
    }

    @Override
    public void close() {
        queryResult.close();
        stream.close();
    }
}
