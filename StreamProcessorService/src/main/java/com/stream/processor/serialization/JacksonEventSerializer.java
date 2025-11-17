package com.stream.processor.serialization;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.stream.processor.Model.SensorEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JacksonEventSerializer implements EventSerializer{
    private final ObjectMapper objectMapper;

    @Override
    public String serialize(SensorEvent event) throws SerializationException{
        try{
            return objectMapper.writeValueAsString(event) ;
        }catch(Exception e){
            throw new SerializationException("Failed to serialize event", e);
        }
    }

    @Override
    public SensorEvent deserialize(String json) throws SerializationException {
        try {
            return objectMapper.readValue(json, SensorEvent.class);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize event", e);
        }
    }
}
