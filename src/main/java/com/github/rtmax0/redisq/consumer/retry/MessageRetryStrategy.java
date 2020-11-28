package com.github.rtmax0.redisq.consumer.retry;

import com.github.rtmax0.redisq.Message;
import com.github.rtmax0.redisq.MessageQueue;

public interface MessageRetryStrategy<T> {

    void retry(Message<T> message, MessageQueue queue, String consumerId);
}
