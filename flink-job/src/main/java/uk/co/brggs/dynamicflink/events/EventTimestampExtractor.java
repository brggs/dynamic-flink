package uk.co.brggs.dynamicflink.events;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import java.time.Instant;

@Slf4j
public class EventTimestampExtractor implements SerializableTimestampAssigner<String> {

    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        try {
            val inputEvent = new InputEvent(element);
            val date = inputEvent.getField(InputEvent.EventTimeField);
            return Instant.parse(date).toEpochMilli();
        } catch (Exception e) {
            log.warn("Failed to extract timestamp from event: " + element, e);
            return System.currentTimeMillis();
        }
    }
}
