package com.github.rtmax0.redisq.producer;

import com.github.rtmax0.redisq.Message;
import com.github.rtmax0.redisq.MessageQueue;
import com.github.rtmax0.redisq.persistence.RedisOps;
import org.apache.commons.lang.StringUtils;

import javax.annotation.PostConstruct;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class DefaultMessageProducer<T> implements MessageProducer<T> {

    private RedisOps redisOps;

    private MessageQueue queue;

    private long defaultTimeToLive = 1;
    private TimeUnit defaultTimeToLiveUnit = TimeUnit.DAYS;
    private SubmissionStrategy submissionStrategy;

    @PostConstruct
    public void initialize() {
        if (submissionStrategy == null) {
            submissionStrategy = new MultiConsumerSubmissionStrategy(redisOps);
        }
    }

    public void submit(T payload) {
        create(payload).submit();
    }

    public MessageSender<T> create(T payload) {
        this.initialize();
        return new DefaultMessageSender(this, payload);
    }

    private void submit(Message<T> message) {
        submissionStrategy.submit(queue, message);
    }

    private void submit(Message<T> message, String consumer) {
        submissionStrategy.submit(queue, message, consumer);
    }

    public void setRedisOps(RedisOps redisOps) {
        this.redisOps = redisOps;
    }

    public void setSubmissionStrategy(SubmissionStrategy submissionStrategy) {
        this.submissionStrategy = submissionStrategy;
    }

    public void setDefaultTimeToLive(long defaultTimeToLive) {
        this.defaultTimeToLive = defaultTimeToLive;
    }

    public void setDefaultTimeToLiveUnit(TimeUnit defaultTimeToLiveUnit) {
        this.defaultTimeToLiveUnit = defaultTimeToLiveUnit;
    }

    public void setQueue(MessageQueue queue) {
        this.queue = queue;
    }

    private class DefaultMessageSender implements MessageSender<T> {
        private T payload;
        private long timeToLive;
        private TimeUnit timeToLiveUnit;
        private String targetConsumer;

        private DefaultMessageProducer<T> producer;

        private DefaultMessageSender(DefaultMessageProducer<T> producer, T payload) {
            this.payload = payload;
            this.timeToLive = defaultTimeToLive;
            this.timeToLiveUnit = defaultTimeToLiveUnit;
            this.producer = producer;
        }

        public MessageSender<T> withTimeToLive(long time, TimeUnit unit) {
            this.timeToLive = time;
            this.timeToLiveUnit = unit;
            return this;
        }

        public MessageSender<T> withTargetConsumer(String consumerId) {
            this.targetConsumer = consumerId;
            return this;
        }

        public void submit() {
            long ttlSeconds = timeToLiveUnit.toSeconds(timeToLive);

            Message<T> message = new Message<T>();
            message.setCreation(Calendar.getInstance());
            message.setPayload(payload);
            message.setTimeToLiveSeconds(ttlSeconds);

            redisOps.addMessage(queue.getQueueName(), message);

            System.out.println("redis=" + this.producer.redisOps);
            this.producer.initialize();
            if (StringUtils.isNotEmpty(targetConsumer)) {
                this.producer.submit(message, targetConsumer);
            } else {
                this.producer.submit(message);
            }
        }
    }
}
