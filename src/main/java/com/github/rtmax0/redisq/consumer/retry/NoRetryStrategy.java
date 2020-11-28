package com.github.rtmax0.redisq.consumer.retry;

import com.github.rtmax0.redisq.Message;
import com.github.rtmax0.redisq.MessageQueue;

public class NoRetryStrategy<T> implements MessageRetryStrategy<T> {

    public void retry(Message<T> message, MessageQueue queue, String consumerId) {
        /* no-op */
    }
}
