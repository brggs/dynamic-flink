package uk.co.brggs.dynamicflink.events;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Instant;

@Slf4j
public class EventTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<String> {

    public EventTimestampExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(String element) {
        try {
            val inputEvent = new InputEvent(element);
            val date = inputEvent.getField(InputEvent.EventTimeField);
            return Instant.parse(date).toEpochMilli();
        } catch (Exception e) {
            log.warn("Failed to extract timestamp from event.");
            return System.currentTimeMillis();
        }
    }
}

