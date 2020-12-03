package com.github.rtmax0.redisq.exception;

/**
 * Handy class for wrapping runtime Exceptions with a root cause.
 */
public class NestedRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -6627352352112612765L;

    public NestedRuntimeException(String msg) {
        super(msg);
    }

    public NestedRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public String getMessage() {
        return buildMessage(super.getMessage(), this.getCause());
    }

    public Throwable getRootCause() {
        return getRootCause(this);
    }

    public Throwable getMostSpecificCause() {
        Throwable rootCause = this.getRootCause();
        return (Throwable)(rootCause != null ? rootCause : this);
    }

    public boolean contains(Class<?> exType) {
        if (exType == null) {
            return false;
        } else if (exType.isInstance(this)) {
            return true;
        } else {
            Throwable cause = this.getCause();
            if (cause == this) {
                return false;
            } else if (cause instanceof NestedRuntimeException) {
                return ((NestedRuntimeException)cause).contains(exType);
            } else {
                while(cause != null) {
                    if (exType.isInstance(cause)) {
                        return true;
                    }

                    if (cause.getCause() == cause) {
                        break;
                    }

                    cause = cause.getCause();
                }

                return false;
            }
        }
    }

    private static String buildMessage(String message, Throwable cause) {
        if (cause == null) {
            return message;
        }
        StringBuilder sb = new StringBuilder(64);
        if (message != null) {
            sb.append(message).append("; ");
        }
        sb.append("nested exception is ").append(cause);
        return sb.toString();
    }

    /**
     * Retrieve the innermost cause of the given exception, if any.
     */
    private static Throwable getRootCause(Throwable original) {
        if (original == null) {
            return null;
        }
        Throwable rootCause = null;
        Throwable cause = original.getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }

}
