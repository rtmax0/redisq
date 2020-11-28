package com.github.rtmax0.redisq.consumer;

import com.github.rtmax0.redisq.Message;
import com.github.rtmax0.redisq.consumer.retry.RetryableMessageException;

public interface MessageListener<T> {

    void onMessage(Message<T> message) throws RetryableMessageException;
}
