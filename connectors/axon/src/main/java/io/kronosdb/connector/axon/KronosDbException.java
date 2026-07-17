package io.kronosdb.connector.axon;

/**
 * Base exception for KronosDB connector errors.
 */
public class KronosDbException extends RuntimeException {

    private final String errorCode;

    public KronosDbException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public KronosDbException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
