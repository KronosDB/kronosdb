package io.kronosdb.connector.axon.event;

import com.google.protobuf.ByteString;
import io.kronosdb.grpc.eventstore.Event;
import io.kronosdb.grpc.eventstore.Tag;
import io.kronosdb.grpc.eventstore.TaggedEvent;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts between Axon Framework {@link TaggedEventMessage} and KronosDB gRPC {@link TaggedEvent}.
 * <p>
 * Used during append (framework to gRPC) and source/stream (gRPC to framework) operations.
 */
public class TaggedEventConverter {

    private final EventConverter converter;

    public TaggedEventConverter(EventConverter converter) {
        this.converter = Objects.requireNonNull(converter, "The EventConverter cannot be null.");
    }

    /**
     * Converts an Axon Framework tagged event to a KronosDB gRPC tagged event (for appending).
     */
    public TaggedEvent convertTaggedEventMessage(TaggedEventMessage<?> taggedEvent) {
        return TaggedEvent.newBuilder()
                .setEvent(convertEventMessage(taggedEvent.event()))
                .addAllTags(convertTags(taggedEvent.tags()))
                .build();
    }

    private Event convertEventMessage(EventMessage eventMessage) {
        return Event.newBuilder()
                .setIdentifier(eventMessage.identifier())
                .setTimestamp(eventMessage.timestamp().toEpochMilli())
                .setName(eventMessage.type().name())
                .setVersion(eventMessage.type().version())
                .setPayload(convertPayload(eventMessage))
                .putAllMetadata(convertMetadata(eventMessage.metadata()))
                .build();
    }

    private ByteString convertPayload(EventMessage eventMessage) {
        byte[] bytes = eventMessage.payloadAs(byte[].class, converter);
        return bytes == null || bytes.length == 0 ? ByteString.EMPTY : ByteString.copyFrom(bytes);
    }

    private Map<String, String> convertMetadata(Metadata metadata) {
        return metadata.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static List<Tag> convertTags(Set<org.axonframework.messaging.eventstreaming.Tag> tags) {
        return tags.stream()
                .map(TaggedEventConverter::convertTag)
                .collect(Collectors.toList());
    }

    private static Tag convertTag(org.axonframework.messaging.eventstreaming.Tag tag) {
        return Tag.newBuilder()
                .setKey(ByteString.copyFrom(tag.key(), StandardCharsets.UTF_8))
                .setValue(ByteString.copyFrom(tag.value(), StandardCharsets.UTF_8))
                .build();
    }

    /**
     * Converts a KronosDB gRPC event to an Axon Framework event message (for sourcing/streaming).
     */
    public EventMessage convertEvent(Event event) {
        return new GenericEventMessage(
                event.getIdentifier(),
                new MessageType(event.getName(), event.getVersion()),
                event.getPayload().toByteArray(),
                event.getMetadataMap(),
                Instant.ofEpochMilli(event.getTimestamp())
        ).withConverter(converter);
    }
}
