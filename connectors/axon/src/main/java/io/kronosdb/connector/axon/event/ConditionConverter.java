package io.kronosdb.connector.axon.event;

import com.google.protobuf.ByteString;
import io.kronosdb.grpc.eventstore.ConsistencyCondition;
import io.kronosdb.grpc.eventstore.Criterion;
import io.kronosdb.grpc.eventstore.SourceRequest;
import io.kronosdb.grpc.eventstore.Tag;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.EventCriterion;
import org.axonframework.messaging.eventstreaming.StreamingCondition;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Converts Axon Framework event conditions (append, sourcing, streaming) into
 * KronosDB gRPC request messages.
 */
public final class ConditionConverter {

    private ConditionConverter() {
    }

    /**
     * Converts an {@link AppendCondition} to a KronosDB {@link ConsistencyCondition}.
     */
    public static ConsistencyCondition convertAppendCondition(AppendCondition condition) {
        long marker = GlobalIndexConsistencyMarker.position(condition.consistencyMarker());
        if (marker < 0) {
            marker = 0;
        }
        return ConsistencyCondition.newBuilder()
                .setConsistencyMarker(marker)
                .addAllCriteria(convertEventCriteria(condition.criteria().flatten()))
                .build();
    }

    /**
     * Converts a {@link SourcingCondition} to a KronosDB {@link SourceRequest}.
     */
    public static SourceRequest convertSourcingCondition(SourcingCondition condition) {
        long fromIndex = GlobalIndexPosition.toIndex(condition.start());
        // GlobalIndexPosition.toIndex returns Long.MIN_VALUE for Position.START.
        // KronosDB uses 0 to mean "start from the beginning".
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        return SourceRequest.newBuilder()
                .setFromSequence(fromIndex)
                .addAllCriteria(convertEventCriteria(condition.criteria().flatten()))
                .build();
    }

    /**
     * Extracts the starting sequence position from a {@link StreamingCondition}.
     */
    public static long extractStreamFromSequence(StreamingCondition condition) {
        return condition.position().position().orElse(0);
    }

    /**
     * Extracts the criteria from a {@link StreamingCondition} as KronosDB {@link Criterion} list.
     */
    public static List<Criterion> extractStreamCriteria(StreamingCondition condition) {
        return convertEventCriteria(condition.criteria().flatten());
    }

    private static List<Criterion> convertEventCriteria(Set<EventCriterion> eventCriteria) {
        return eventCriteria.stream()
                .map(ConditionConverter::convertEventCriterion)
                .toList();
    }

    private static Criterion convertEventCriterion(EventCriterion eventCriterion) {
        return Criterion.newBuilder()
                .addAllNames(convertTypes(eventCriterion.types()))
                .addAllTags(convertTags(eventCriterion.tags()))
                .build();
    }

    private static List<Tag> convertTags(Set<org.axonframework.messaging.eventstreaming.Tag> tags) {
        return tags.stream()
                .map(ConditionConverter::convertTag)
                .toList();
    }

    private static Tag convertTag(org.axonframework.messaging.eventstreaming.Tag tag) {
        return Tag.newBuilder()
                .setKey(ByteString.copyFrom(tag.key(), StandardCharsets.UTF_8))
                .setValue(ByteString.copyFrom(tag.value(), StandardCharsets.UTF_8))
                .build();
    }

    private static List<String> convertTypes(Set<QualifiedName> types) {
        return types.stream().map(QualifiedName::name).toList();
    }
}
