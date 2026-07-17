package io.kronosdb.connector.axon;

import io.kronosdb.grpc.ErrorMessage;

import static java.util.Arrays.stream;

/**
 * Maps KronosDB error codes to Axon Framework exceptions.
 * <p>
 * Maps KronosDB error codes to appropriate exceptions.
 */
public enum ErrorCode {

    // Authentication
    AUTHENTICATION_TOKEN_MISSING("KRONOS-1000"),
    AUTHENTICATION_INVALID_TOKEN("KRONOS-1001"),

    // Event store errors
    CONSISTENCY_CONDITION_VIOLATED("KRONOS-2000"),
    NO_EVENT_STORE_LEADER("KRONOS-2100"),
    EVENT_PAYLOAD_TOO_LARGE("KRONOS-2001"),

    // Communication errors
    CONNECTION_FAILED("KRONOS-3001"),
    GRPC_MESSAGE_TOO_LARGE("KRONOS-3002"),

    // Command errors
    NO_HANDLER_FOR_COMMAND("KRONOS-4000"),
    COMMAND_EXECUTION_ERROR("KRONOS-4002"),
    COMMAND_DISPATCH_ERROR("KRONOS-4003"),

    // Query errors
    NO_HANDLER_FOR_QUERY("KRONOS-5000"),
    QUERY_EXECUTION_ERROR("KRONOS-5001"),
    QUERY_DISPATCH_ERROR("KRONOS-5002"),

    // Internal errors
    DATAFILE_READ_ERROR("KRONOS-9000"),
    DATAFILE_WRITE_ERROR("KRONOS-9100"),

    // Default
    OTHER("KRONOS-0001");

    private final String code;

    ErrorCode(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    /**
     * Resolves an ErrorCode from its string code.
     */
    public static ErrorCode fromCode(String code) {
        return stream(values())
                .filter(v -> v.code.equals(code))
                .findFirst()
                .orElse(OTHER);
    }

    /**
     * Converts a KronosDB gRPC error to a {@link KronosDbException}.
     */
    public KronosDbException toException(ErrorMessage errorMessage) {
        return new KronosDbException(code, errorMessage.getMessage());
    }

    /**
     * Converts a throwable to a {@link KronosDbException} with this error code.
     */
    public KronosDbException toException(Throwable throwable) {
        return new KronosDbException(code, throwable.getMessage(), throwable);
    }
}
