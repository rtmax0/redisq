package com.github.rtmax0.redisq.persistence;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Jedis Template Methods.
 */
public class JedisWrapper {
    private final Pool<Jedis> jedisPool;

    public JedisWrapper(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void execute(Consumer<Jedis> consumer) {
        try (Jedis jedis = this.jedisPool.getResource()) {
            consumer.accept(jedis);
        }
    }

    public <R> R executeAndReturn(Function<Jedis, R> function) {
        try (Jedis jedis = this.jedisPool.getResource()) {
            return function.apply(jedis);
        }
    }
}
