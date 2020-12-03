package com.github.rtmax0.redisq.exception;

/**
 * Generic exception indicating a serialization/deserialization error.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
public class SerializationException extends NestedRuntimeException {

    /**
     * Constructs a new {@link SerializationException} instance.
     *
     * @param msg
     */
    public SerializationException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new {@link SerializationException} instance.
     *
     * @param msg the detail message.
     * @param cause the nested exception.
     */
    public SerializationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
