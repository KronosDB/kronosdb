package io.kronosdb.connector.axon.command;

import com.google.protobuf.ByteString;
import io.kronosdb.connector.axon.MetadataConverter;
import io.kronosdb.grpc.MetadataValue;
import io.kronosdb.grpc.ProcessingInstruction;
import io.kronosdb.grpc.ProcessingKey;
import io.kronosdb.grpc.SerializedObject;
import io.kronosdb.grpc.command.Command;
import io.kronosdb.grpc.command.CommandResponse;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Utility class for converting between Axon Framework command messages
 * and KronosDB gRPC command messages.
 */
public final class CommandConverter {

    private CommandConverter() {
    }

    /**
     * Converts an Axon Framework {@link CommandMessage} to a KronosDB gRPC {@link Command}.
     */
    public static Command convertCommandMessage(CommandMessage command, String clientId, String componentName) {
        Object payload = command.payload();
        if (!(payload instanceof byte[] payloadAsBytes)) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for KronosDB connector, but was: "
                            + command.payloadType().getName());
        }

        Command.Builder builder = Command.newBuilder();
        addRoutingKey(builder, command);
        addPriority(builder, command);

        return builder.setClientId(clientId)
                .setComponentName(componentName)
                .setMessageIdentifier(command.identifier())
                .setName(command.type().name())
                .putAllMetadata(MetadataConverter.toGrpcMetadata(command.metadata()))
                .setPayload(SerializedObject.newBuilder()
                        .setData(ByteString.copyFrom(payloadAsBytes))
                        .setType(command.type().name())
                        .setRevision(command.type().version())
                        .build())
                .build();
    }

    /**
     * Converts a KronosDB gRPC {@link CommandResponse} to an Axon Framework {@link CommandResultMessage}.
     */
    public static CompletableFuture<CommandResultMessage> convertCommandResponse(
            CommandResponse commandResponse, @Nullable Converter converter) {
        if (commandResponse.hasErrorMessage() && !commandResponse.getErrorCode().isEmpty()) {
            return CompletableFuture.failedFuture(
                    new KronosDbCommandDispatchException(
                            commandResponse.getErrorCode(),
                            commandResponse.getErrorMessage().getMessage()));
        }

        if (commandResponse.getPayload().getType().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        MessageType messageType = new MessageType(
                commandResponse.getPayload().getType(),
                commandResponse.getPayload().getRevision());
        Map<String, String> metadata = MetadataConverter.fromGrpcMetadata(commandResponse.getMetadataMap());
        return CompletableFuture.completedFuture(new GenericCommandResultMessage(new GenericMessage(
                commandResponse.getMessageIdentifier(),
                messageType,
                commandResponse.getPayload().getData().toByteArray(),
                metadata
        )).withConverter(converter));
    }

    /**
     * Converts a KronosDB gRPC {@link Command} to an Axon Framework {@link CommandMessage}.
     */
    public static CommandMessage convertCommand(Command command, @Nullable Converter converter) {
        SerializedObject commandPayload = command.getPayload();
        int priority = priority(command.getProcessingInstructionsList());
        String routingKey = routingKey(command.getProcessingInstructionsList());
        return new GenericCommandMessage(
                new GenericMessage(
                        command.getMessageIdentifier(),
                        new MessageType(commandPayload.getType(), commandPayload.getRevision()),
                        commandPayload.getData().toByteArray(),
                        MetadataConverter.fromGrpcMetadata(command.getMetadataMap())
                ),
                routingKey,
                priority
        ).withConverter(converter);
    }

    /**
     * Converts an Axon Framework {@link CommandResultMessage} to a KronosDB gRPC {@link CommandResponse}.
     */
    public static CommandResponse convertResultMessage(@Nullable CommandResultMessage resultMessage,
                                                        String requestIdentifier) {
        if (resultMessage == null) {
            return CommandResponse.newBuilder()
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setRequestIdentifier(requestIdentifier)
                    .build();
        }
        Object payload = resultMessage.payload();
        String messageId = Objects.requireNonNullElse(resultMessage.identifier(), UUID.randomUUID().toString());
        CommandResponse.Builder responseBuilder = CommandResponse.newBuilder()
                .setMessageIdentifier(messageId)
                .putAllMetadata(MetadataConverter.toGrpcMetadata(resultMessage.metadata()))
                .setRequestIdentifier(requestIdentifier);

        if (payload != null && !(payload instanceof byte[])) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for KronosDB connector, but was: "
                            + resultMessage.payloadType().getName());
        }
        byte[] payloadAsBytes = (byte[]) Objects.requireNonNullElse(payload, new byte[0]);
        return responseBuilder.setPayload(SerializedObject.newBuilder()
                        .setType(resultMessage.type().name())
                        .setRevision(resultMessage.type().version())
                        .setData(ByteString.copyFrom(payloadAsBytes)))
                .build();
    }

    private static void addRoutingKey(Command.Builder builder, CommandMessage command) {
        command.routingKey().ifPresent(routingKey ->
                builder.addProcessingInstructions(ProcessingInstruction.newBuilder()
                        .setKey(ProcessingKey.ROUTING_KEY)
                        .setValue(MetadataValue.newBuilder().setTextValue(routingKey))
                        .build()));
    }

    private static void addPriority(Command.Builder builder, CommandMessage command) {
        command.priority().ifPresent(priority ->
                builder.addProcessingInstructions(ProcessingInstruction.newBuilder()
                        .setKey(ProcessingKey.PRIORITY)
                        .setValue(MetadataValue.newBuilder().setNumberValue(priority))
                        .build()));
    }

    private static int priority(java.util.List<ProcessingInstruction> instructions) {
        return instructions.stream()
                .filter(pi -> pi.getKey() == ProcessingKey.PRIORITY)
                .findFirst()
                .map(pi -> (int) pi.getValue().getNumberValue())
                .orElse(0);
    }

    private static String routingKey(java.util.List<ProcessingInstruction> instructions) {
        return instructions.stream()
                .filter(pi -> pi.getKey() == ProcessingKey.ROUTING_KEY)
                .findFirst()
                .map(pi -> pi.getValue().getTextValue())
                .orElse("");
    }
}
