package com.github.rtmax0.redisq.cucumber.steps;

import com.github.rtmax0.redisq.persistence.JedisWrapper;
import cucumber.api.java.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.BinaryJedis;

public class RedisHooks extends Steps {

//    @Autowired
//    private RedisTemplate redisTemplate;

    @Autowired
    private JedisWrapper jedisWrapper;


    @Qualifier("jedisConnectionFactory")
    @Autowired
    private JedisConnectionFactory jedisConnectionFactory;

    @Before
    @SuppressWarnings("unchecked")
    public void setupGlobal() {
        jedisConnectionFactory.setDatabase(2);
//
//        redisTemplate.execute((RedisCallback<Object>) connection -> {
//            connection.flushDb();
//            return null;
//        });

        jedisWrapper.execute(BinaryJedis::flushDB);
    }
}
