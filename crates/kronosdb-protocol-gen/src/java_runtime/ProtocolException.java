package io.kronosdb.protocol;

/**
 * Bytes that violate the protocol: a truncated frame, an invalid UTF-8
 * string, a bool or option byte other than 0 or 1, a union tag this version
 * does not define; or, when encoding, a value that does not fit its field.
 */
public final class ProtocolException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ProtocolException(String message) {
        super(message);
    }
}
