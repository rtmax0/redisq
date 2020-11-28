package com.github.rtmax0.redisq.producer;

import java.util.concurrent.TimeUnit;

public interface MessageSender<T> {

    MessageSender<T> withTimeToLive(long time, TimeUnit unit);

    MessageSender<T> withTargetConsumer(String consumerId);

    void submit();
}
