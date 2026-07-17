package io.kronosdb.connector.axon.query;

import io.kronosdb.connector.grpc.ResultStream;
import io.kronosdb.grpc.query.QueryResponse;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * A {@link MessageStream} that wraps a KronosDB {@link ResultStream} of {@link QueryResponse}
 * messages, converting them to Axon Framework {@link QueryResponseMessage}s.
 */
public class QueryResponseMessageStream implements MessageStream<QueryResponseMessage> {

    private final ResultStream<QueryResponse> stream;
    private final @Nullable Converter converter;

    public QueryResponseMessageStream(ResultStream<QueryResponse> stream, @Nullable Converter converter) {
        this.stream = stream;
        this.converter = converter;
    }

    @Override
    public Optional<Entry<QueryResponseMessage>> next() {
        QueryResponse response = stream.nextIfAvailable();
        if (response == null) {
            return Optional.empty();
        }
        return Optional.of(new SimpleEntry<>(QueryConverter.convertQueryResponse(response, converter)));
    }

    @Override
    public Optional<Entry<QueryResponseMessage>> peek() {
        QueryResponse response = stream.peek();
        if (response == null) {
            return Optional.empty();
        }
        return Optional.of(new SimpleEntry<>(QueryConverter.convertQueryResponse(response, converter)));
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
        return stream.isClosed();
    }

    @Override
    public boolean hasNextAvailable() {
        return stream.peek() != null;
    }

    @Override
    public void close() {
        stream.close();
    }
}
