package uk.co.brggs.dynamicflink.control;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class ControlOutputSerializationSchema implements SerializationSchema<ControlOutput> {
    @Override
    public byte[] serialize(ControlOutput element) {
        try {
            return new ObjectMapper().writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialise ControlOutput", e);
            return null;
        }
    }
}
