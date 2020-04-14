package uk.co.brggs.dynamicflink.outputevents;

import com.jsoniter.output.JsonStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;

@Slf4j
public class OutputEventSerializationSchema implements SerializationSchema<OutputEvent> {
    @Override
    public byte[] serialize(OutputEvent element) {
        try {
            return JsonStream.serialize(element).getBytes();
        } catch (Exception e) {
            log.error("Failed to serialise OutputEvent", e);
            return null;
        }
    }
}
