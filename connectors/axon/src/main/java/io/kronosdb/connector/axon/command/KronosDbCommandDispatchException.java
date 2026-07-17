package io.kronosdb.connector.axon.command;

import io.kronosdb.connector.axon.KronosDbException;

/**
 * Exception thrown when a command dispatch to KronosDB fails.
 */
public class KronosDbCommandDispatchException extends KronosDbException {

    public KronosDbCommandDispatchException(String errorCode, String message) {
        super(errorCode, message);
    }

    public KronosDbCommandDispatchException(String errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
