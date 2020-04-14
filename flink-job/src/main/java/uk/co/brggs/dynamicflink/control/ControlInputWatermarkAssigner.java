package uk.co.brggs.dynamicflink.control;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Assigns the maximum watermark, allowing the event stream to control watermark progression.
 */
public class ControlInputWatermarkAssigner implements AssignerWithPeriodicWatermarks<ControlInput> {
    @Override
    public Watermark getCurrentWatermark() {
        return Watermark.MAX_WATERMARK;
    }

    @Override
    public long extractTimestamp(ControlInput element, long previousElementTimestamp) {
        return System.currentTimeMillis();
    }
}
