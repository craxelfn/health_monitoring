package com.stream.processor.serialization;

import com.stream.processor.Model.SensorEvent;

public interface EventSerializer {

    String serialize(SensorEvent event) throws SerializationException  ;
    SensorEvent deserialize(String json) throws SerializationException;
}
