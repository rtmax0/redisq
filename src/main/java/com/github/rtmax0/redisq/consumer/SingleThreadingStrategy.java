package com.github.rtmax0.redisq.consumer;

public class SingleThreadingStrategy extends MultiThreadingStrategy {

    public SingleThreadingStrategy() {
        super(1);
    }
}
