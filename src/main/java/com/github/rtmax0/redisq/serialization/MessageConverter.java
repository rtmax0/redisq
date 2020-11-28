package com.github.rtmax0.redisq.serialization;

import com.github.rtmax0.redisq.Message;

import java.util.Map;

public interface MessageConverter {

    <T> Map<String, String> toMap(Message<T> message, PayloadSerializer payloadSerializer);

    <T> Message<T> toMessage(Map<String, String> data, Class<T> payloadType, PayloadSerializer payloadSerializer);
}
