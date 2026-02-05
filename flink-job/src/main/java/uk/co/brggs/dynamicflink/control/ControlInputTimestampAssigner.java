package uk.co.brggs.dynamicflink.control;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

/**
 * Assigns timestamps to control input messages.
 */
public class ControlInputTimestampAssigner implements SerializableTimestampAssigner<ControlInput> {
    @Override
    public long extractTimestamp(ControlInput element, long recordTimestamp) {
        try {
            return java.time.Instant.parse(element.getTimestamp()).toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }
}
