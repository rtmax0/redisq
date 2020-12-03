package com.github.rtmax0.redisq.persistence;

import com.github.rtmax0.redisq.Message;
import com.github.rtmax0.redisq.MessageQueue;
import com.github.rtmax0.redisq.serialization.DefaultMessageConverter;
import com.github.rtmax0.redisq.serialization.GsonPayloadSerializer;
import com.github.rtmax0.redisq.serialization.MessageConverter;
import com.github.rtmax0.redisq.serialization.PayloadSerializer;
import com.github.rtmax0.redisq.utils.KeysFactory;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
public class RedisOps {
//    private RedisTemplate redisTemplate;

    private JedisWrapper jedisWrapper;

    private PayloadSerializer payloadSerializer = new GsonPayloadSerializer();

    private MessageConverter  messageConverter  = new DefaultMessageConverter();

    public void ensureConsumerRegistered(String queueName, String consumerId) {
        jedisWrapper.execute((jedis) -> jedis.set(KeysFactory.keyForRegisteredConsumers(queueName), consumerId));
    }

    public Collection<String> getRegisteredConsumers(String queueName) {
        return jedisWrapper.executeAndReturn((jedis) -> jedis.smembers(KeysFactory.keyForRegisteredConsumers(queueName)));
    }

    public <T> void addMessage(String queueName, Message<T> message) {
        assert message != null;
        assert message.getTimeToLiveSeconds() != null;

        String msgId = generateNextMessageID(queueName);
        message.setId(msgId);
        if (message.getCreation() == null) {
            message.setCreation(Calendar.getInstance());
        }

        saveMessage(queueName, message);
    }

    public <T> void saveMessage(String queueName, Message<T> message) {
        assert message != null;
        assert message.getId() != null;
        assert message.getTimeToLiveSeconds() != null;

        Map<String, String> asMap = messageConverter.toMap(message, payloadSerializer);

        String messageKey = KeysFactory.keyForMessage(queueName, message.getId());

        jedisWrapper.execute((jedis) -> {
            jedis.hset(messageKey, asMap);
            jedis.pexpire(messageKey, TimeUnit.SECONDS.toMillis(message.getTimeToLiveSeconds()));
        });
    }

    public <T> Message<T> loadMessageById(String queueName, String id, Class<T> payloadType) {
        String messageKey = KeysFactory.keyForMessage(queueName, id);

        return jedisWrapper.executeAndReturn((jedis) -> {
            Map<String, String> messageData = jedis.hgetAll(messageKey);
            return messageConverter.toMessage(messageData, payloadType, payloadSerializer);
        });
    }

    public String dequeueMessageFromHead(String queueName, String consumerId, long timeoutSeconds) {
        String queueKey = KeysFactory.keyForConsumerSpecificQueue(queueName, consumerId);

        // redisTemplate.opsForList().leftPop(key, timeoutSeconds, TimeUnit.SECONDS);
        return jedisWrapper.executeAndReturn(jedis -> {
            List<String> lPop = jedis.blpop((int) timeoutSeconds, queueKey);
            return (lPop != null && lPop.size() > 0) ? lPop.get(0) : null;
        });
    }

    public <T> Message<T> peekNextMessageInQueue(String queueName, String consumerId, Class<T> payloadType) {
        String queueKey = KeysFactory.keyForConsumerSpecificQueue(queueName, consumerId);

        return jedisWrapper.executeAndReturn((jedis) -> {
            String nextId = jedis.lindex(queueKey, 0);
            if (nextId == null) {
                return null;
            }
            return loadMessageById(queueName, nextId, payloadType);
        });
    }

    /**
     * @param rangeStart zero-based index of first item to retrieve
     * @param rangeEnd zero-based index of last item to retrieve
     */
    private <T> List<Message<T>> peekMessagesInQueue(String queueName, String consumerId, long rangeStart, long rangeEnd, Class<T> payloadType) {

        String queueKey = KeysFactory.keyForConsumerSpecificQueue(queueName, consumerId);

        return jedisWrapper.executeAndReturn((jedis) -> {
            List<String> messageIds = jedis.lrange(queueKey, rangeStart, rangeEnd);
            List<Message<T>> messages = new ArrayList<Message<T>>(messageIds.size());
            for (String id : messageIds) {
                messages.add(loadMessageById(queueName, id, payloadType));
            }
            return messages;
        });
    }

    /**
     * Peeks messages in the specified queue (for the default consumer).
     * @param rangeStart zero-based index of first item to retrieve
     * @param rangeEnd zero-based index of last item to retrieve
     */
    public <T> List<Message<T>> peekMessagesInQueue(MessageQueue queue, long rangeStart, long rangeEnd, Class<T> payloadType) {
        return peekMessagesInQueue(queue.getQueueName(), queue.getDefaultConsumerId(), rangeStart, rangeEnd, payloadType);
    }

    public void emptyQueue(String queueName) {
        Collection<String> consumerIds = getRegisteredConsumers(queueName);

        jedisWrapper.execute((jedis) -> {
            for (String consumerId : consumerIds) {
                String queueKey = KeysFactory.keyForConsumerSpecificQueue(queueName, consumerId);
                jedis.del(queueKey);
            }
        });
    }

    private String generateNextMessageID(String queueName) {
        return jedisWrapper.executeAndReturn((jedis) -> Long.toString(jedis.incrBy(KeysFactory.keyForNextID(queueName), 1)));
    }

    public Long getQueueSizeForConsumer(String queueName, String consumerId) {
        return jedisWrapper.executeAndReturn((jedis) -> jedis.llen(KeysFactory.keyForConsumerSpecificQueue(queueName, consumerId)));

    }

    public void enqueueMessageAtTail(String queueName, String consumerId, String messageId) {
        if (StringUtils.isEmpty(messageId)) {
            throw new IllegalArgumentException("Message must have been persisted before being enqueued.");
        }

        String key = KeysFactory.keyForConsumerSpecificQueue(queueName, consumerId);
        jedisWrapper.execute((jedis) -> {
            jedis.rpush(key, messageId);
        });
    }

    public void enqueueMessageInSet(String queueName, String consumerId, String messageId) {
        if (StringUtils.isEmpty(messageId)) {
            throw new IllegalArgumentException("Message must have been persisted before being enqueued.");
        }

        String key = KeysFactory.keyForConsumerSpecificQueue(queueName, consumerId);

        jedisWrapper.execute((jedis) -> {
            jedis.sadd(key, messageId);
        });
    }

    public void notifyWaitersOnSet(String queueName, String consumerId) {
        String key = KeysFactory.keyForConsumerSpecificQueueNotificationList(queueName, consumerId);

        jedisWrapper.execute(jedis -> {
            jedis.rpush(key, "x");
        });
    }

    public void waitOnSet(String queueName, String consumerId, long timeoutSeconds) {
        String key = KeysFactory.keyForConsumerSpecificQueueNotificationList(queueName, consumerId);

        // redisTemplate.opsForList().leftPop(key, timeoutSeconds, TimeUnit.SECONDS);
        jedisWrapper.executeAndReturn(jedis -> {
            List<String> lPop = jedis.blpop((int) timeoutSeconds, key);
            return (lPop != null && lPop.size() > 0) ? lPop.get(0) : null;
        });
    }

    public String randomPopFromSet(String queueName, String consumerId) {
        return jedisWrapper.executeAndReturn((jedis) -> {
            String key = KeysFactory.keyForConsumerSpecificQueue(queueName, consumerId);
            return jedis.spop(key);
        });
    }

    public boolean tryObtainLockForQueue(String queueName, String consumerId, long expirationTimeout, TimeUnit unit) {
        return jedisWrapper.executeAndReturn((jedis) -> {
            String queueKey = KeysFactory.keyForConsumerSpecificQueueLock(queueName, consumerId);
//            BoundValueOperations<String, Integer> ops = redisTemplate.boundValueOps(queueKey);
//            boolean lockAcquired = ops.setIfAbsent(1);
            Long setnx = jedis.setnx(queueKey, "1");
            boolean lockAcquired = (setnx == 1);

            if (lockAcquired) {
                jedis.pexpire(queueKey, unit.toMillis(expirationTimeout));
                return true;
            }
            return false;
        });
    }

    public void releaseLockForQueue(String queueName, String consumerId) {
        jedisWrapper.execute((jedis) -> {
            jedis.del(KeysFactory.keyForConsumerSpecificQueueLock(queueName, consumerId));
        });
    }

    public void setJedisWrapper(JedisWrapper jedisWrapper) {
        this.jedisWrapper = jedisWrapper;
    }
}
