package com.github.rtmax0.redisq.exception;

/**
 * Fatal exception thrown when the Redis connection fails completely.
 */
public class RedisConnectionFailureException extends NestedRuntimeException {
    public RedisConnectionFailureException(String msg) {
        super(msg);
    }

    public RedisConnectionFailureException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
