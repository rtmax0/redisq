package com.github.rtmax0.redisq.serialization;

import com.github.rtmax0.redisq.exception.SerializationException;
import org.apache.commons.lang.ObjectUtils;

public class StringPayloadSerializer implements PayloadSerializer {

    public String serialize(Object payload) throws SerializationException {
        return ObjectUtils.toString(payload);
    }

    @SuppressWarnings("unchecked")
    public <T> T deserialize(String payload, Class<T> type) throws SerializationException {
        return (T) payload;
    }
}
