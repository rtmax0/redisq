package com.github.rtmax0.redisq.consumer;

import com.github.rtmax0.redisq.exception.RedisConnectionFailureException;

public interface ConnectionFailureHandler {

    /**
     * Called when a connection failure happens. This method is called in the context of each message processing thread.
     * @param e the details on the connection failure
     */
    void serverConnectionFailed(RedisConnectionFailureException e);
}
