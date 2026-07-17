package io.kronosdb.connector.axon;

import io.kronosdb.grpc.MetadataValue;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to convert between KronosDB gRPC metadata values
 * and simple String key-value maps used by Axon Framework messages.
 */
public final class MetadataConverter {

    private MetadataConverter() {
    }

    /**
     * Converts a map of String key-value pairs to a map of KronosDB {@link MetadataValue} objects.
     */
    public static Map<String, MetadataValue> toGrpcMetadata(Map<String, @Nullable String> source) {
        Map<String, MetadataValue> result = new HashMap<>();
        source.forEach((key, value) -> {
            if (value != null) {
                result.put(key, MetadataValue.newBuilder().setTextValue(value).build());
            }
        });
        return result;
    }

    /**
     * Converts a map of KronosDB {@link MetadataValue} objects to a map of String key-value pairs.
     */
    public static Map<String, String> fromGrpcMetadata(Map<String, MetadataValue> source) {
        Map<String, String> result = new HashMap<>();
        source.forEach((key, value) -> {
            String converted = convertFromMetadataValue(value);
            if (converted != null) {
                result.put(key, converted);
            }
        });
        return result;
    }

    @Nullable
    private static String convertFromMetadataValue(MetadataValue value) {
        return switch (value.getDataCase()) {
            case TEXT_VALUE -> value.getTextValue();
            case DOUBLE_VALUE -> Double.toString(value.getDoubleValue());
            case NUMBER_VALUE -> Long.toString(value.getNumberValue());
            case BOOLEAN_VALUE -> Boolean.toString(value.getBooleanValue());
            default -> null;
        };
    }
}
