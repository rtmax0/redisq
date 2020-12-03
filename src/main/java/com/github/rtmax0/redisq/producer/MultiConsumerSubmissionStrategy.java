package com.github.rtmax0.redisq.producer;

import com.github.rtmax0.redisq.Message;
import com.github.rtmax0.redisq.MessageQueue;
import com.github.rtmax0.redisq.persistence.RedisOps;

import java.util.Collection;

/**
 * Submits messages to all registered consumers on a queue.
 */
public class MultiConsumerSubmissionStrategy extends SingleConsumerSubmissionStrategy {

    public MultiConsumerSubmissionStrategy(RedisOps redisOps) {
        super(redisOps);
    }

    @Override
    public void submit(MessageQueue queue, Message<?> message) {

        Collection<String> allConsumers = redisOps.getRegisteredConsumers(queue.getQueueName());
        if (allConsumers.isEmpty()) {
            // use single consumer behavior
            super.submit(queue, message);
        } else {
            queue.enqueue(message, allConsumers.toArray(new String[allConsumers.size()]));
        }
    }
}
