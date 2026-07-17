package io.kronosdb.connector.axon.query;

import com.google.protobuf.ByteString;
import io.kronosdb.connector.axon.MetadataConverter;
import io.kronosdb.grpc.MetadataValue;
import io.kronosdb.grpc.ProcessingInstruction;
import io.kronosdb.grpc.ProcessingKey;
import io.kronosdb.grpc.SerializedObject;
import io.kronosdb.grpc.query.QueryRequest;
import io.kronosdb.grpc.query.QueryResponse;
import io.kronosdb.grpc.query.QueryUpdate;
import io.kronosdb.grpc.query.SubscriptionQuery;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for converting between Axon Framework query messages
 * and KronosDB gRPC query messages.
 */
public final class QueryConverter {

    private QueryConverter() {
    }

    /**
     * Converts a KronosDB gRPC {@link QueryRequest} to an Axon Framework {@link QueryMessage}.
     */
    static QueryMessage convertQueryRequest(QueryRequest queryRequest, @Nullable Converter converter) {
        var payload = queryRequest.getPayload();
        Integer priority = priority(queryRequest.getProcessingInstructionsList());

        var type = new MessageType(payload.getType(), payload.getRevision());
        return new GenericQueryMessage(
                new GenericMessage(
                        queryRequest.getMessageIdentifier(),
                        type,
                        payload.getData().toByteArray(),
                        MetadataConverter.fromGrpcMetadata(queryRequest.getMetadataMap())
                ),
                priority
        ).withConverter(converter);
    }

    /**
     * Converts an Axon Framework {@link QueryMessage} to a KronosDB gRPC {@link QueryRequest}.
     */
    public static QueryRequest convertQueryMessage(QueryMessage query, String clientId, String componentName) {
        Object payload = query.payload();
        if (!(payload instanceof byte[] payloadAsBytes)) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for KronosDB connector, but was: "
                            + query.payloadType().getName());
        }

        QueryRequest.Builder builder = QueryRequest.newBuilder();
        addPriority(builder, query);

        return builder.setTimestamp(System.currentTimeMillis())
                .setClientId(clientId)
                .setComponentName(componentName)
                .setMessageIdentifier(query.identifier())
                .setQuery(query.type().name())
                .putAllMetadata(MetadataConverter.toGrpcMetadata(query.metadata()))
                .setPayload(SerializedObject.newBuilder()
                        .setData(ByteString.copyFrom(payloadAsBytes))
                        .setType(query.type().name())
                        .setRevision(query.type().version())
                        .build())
                .addProcessingInstructions(nrOfResults(1))
                .addProcessingInstructions(timeout(TimeUnit.HOURS.toMillis(1)))
                .build();
    }

    /**
     * Converts a KronosDB gRPC {@link QueryResponse} to an Axon Framework {@link QueryResponseMessage}.
     */
    public static QueryResponseMessage convertQueryResponse(QueryResponse queryResponse,
                                                             @Nullable Converter converter) {
        if (queryResponse.hasErrorMessage() && !queryResponse.getErrorCode().isEmpty()) {
            throw new IllegalArgumentException("Query response contained an error.");
        }
        SerializedObject responsePayload = queryResponse.getPayload();
        var message = new GenericMessage(
                queryResponse.getMessageIdentifier(),
                new MessageType(responsePayload.getType(), responsePayload.getRevision()),
                responsePayload.getData().toByteArray(),
                MetadataConverter.fromGrpcMetadata(queryResponse.getMetadataMap())
        );
        return new GenericQueryResponseMessage(message).withConverter(converter);
    }

    /**
     * Converts an Axon Framework {@link QueryResponseMessage} to a KronosDB gRPC {@link QueryResponse}.
     */
    public static QueryResponse convertQueryResponseMessage(String requestId,
                                                             QueryResponseMessage queryResponseMessage) {
        byte[] payload = Objects.requireNonNullElseGet(
                queryResponseMessage.payloadAs(byte[].class), () -> new byte[0]);
        return QueryResponse.newBuilder()
                .setMessageIdentifier(queryResponseMessage.identifier())
                .setRequestIdentifier(requestId)
                .setPayload(SerializedObject.newBuilder()
                        .setType(queryResponseMessage.type().name())
                        .setRevision(queryResponseMessage.type().version())
                        .setData(ByteString.copyFrom(payload))
                        .build())
                .putAllMetadata(MetadataConverter.toGrpcMetadata(queryResponseMessage.metadata()))
                .build();
    }

    /**
     * Converts a {@link SubscriptionQuery} to a {@link QueryMessage}.
     */
    public static QueryMessage convertSubscriptionQueryMessage(SubscriptionQuery query,
                                                                @Nullable Converter converter) {
        SerializedObject responsePayload = query.getQueryRequest().getPayload();
        var message = new GenericMessage(
                query.getSubscriptionIdentifier(),
                new MessageType(responsePayload.getType(), responsePayload.getRevision()),
                responsePayload.getData().toByteArray(),
                MetadataConverter.fromGrpcMetadata(query.getQueryRequest().getMetadataMap())
        );
        return new GenericQueryMessage(message).withConverter(converter);
    }

    /**
     * Converts a {@link SubscriptionQueryUpdateMessage} to a {@link QueryUpdate}.
     */
    public static QueryUpdate convertQueryUpdate(SubscriptionQueryUpdateMessage update) {
        byte[] payload = Objects.requireNonNullElseGet(update.payloadAs(byte[].class), () -> new byte[0]);
        return QueryUpdate.newBuilder()
                .setMessageIdentifier(update.identifier())
                .setPayload(SerializedObject.newBuilder()
                        .setType(update.type().name())
                        .setRevision(update.type().version())
                        .setData(ByteString.copyFrom(payload))
                        .build())
                .putAllMetadata(MetadataConverter.toGrpcMetadata(update.metadata()))
                .build();
    }

    /**
     * Converts a {@link QueryUpdate} to a {@link SubscriptionQueryUpdateMessage}.
     */
    public static SubscriptionQueryUpdateMessage convertQueryUpdate(QueryUpdate queryUpdate,
                                                                     @Nullable Converter converter) {
        SerializedObject payload = queryUpdate.getPayload();
        var message = new GenericMessage(
                queryUpdate.getMessageIdentifier(),
                new MessageType(payload.getType(), payload.getRevision()),
                payload.getData().toByteArray(),
                MetadataConverter.fromGrpcMetadata(queryUpdate.getMetadataMap())
        );
        return new GenericSubscriptionQueryUpdateMessage(message).withConverter(converter);
    }

    private static void addPriority(QueryRequest.Builder builder, QueryMessage query) {
        query.priority().ifPresent(priority ->
                builder.addProcessingInstructions(ProcessingInstruction.newBuilder()
                        .setKey(ProcessingKey.PRIORITY)
                        .setValue(MetadataValue.newBuilder().setNumberValue(priority))
                        .build()));
    }

    private static ProcessingInstruction nrOfResults(int nrOfResults) {
        return ProcessingInstruction.newBuilder()
                .setKey(ProcessingKey.NR_OF_RESULTS)
                .setValue(MetadataValue.newBuilder().setNumberValue(nrOfResults))
                .build();
    }

    private static ProcessingInstruction timeout(long timeoutMillis) {
        return ProcessingInstruction.newBuilder()
                .setKey(ProcessingKey.TIMEOUT)
                .setValue(MetadataValue.newBuilder().setNumberValue(timeoutMillis))
                .build();
    }

    private static int priority(java.util.List<ProcessingInstruction> instructions) {
        return instructions.stream()
                .filter(pi -> pi.getKey() == ProcessingKey.PRIORITY)
                .findFirst()
                .map(pi -> (int) pi.getValue().getNumberValue())
                .orElse(0);
    }
}
