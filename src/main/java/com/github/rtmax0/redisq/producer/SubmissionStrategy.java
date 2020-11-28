package com.github.rtmax0.redisq.producer;

import com.github.rtmax0.redisq.Message;
import com.github.rtmax0.redisq.MessageQueue;

public interface SubmissionStrategy {

    void submit(MessageQueue queue, Message<?> message);

    void submit(MessageQueue queue, Message<?> message, String consumer);
}
