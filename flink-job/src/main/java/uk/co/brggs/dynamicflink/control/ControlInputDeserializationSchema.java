package uk.co.brggs.dynamicflink.control;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class ControlInputDeserializationSchema implements DeserializationSchema<ControlInput> {

    @Override
    public ControlInput deserialize(byte[] message) {
        try {
            return new ObjectMapper().readValue(message, ControlInput.class);
        } catch (Exception ex) {
            log.error("Error de-serialising ControlInput.", ex);

            // Returning null causes Flink to skip this content
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(ControlInput nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ControlInput> getProducedType() {
        return TypeExtractor.getForClass(ControlInput.class);
    }
}