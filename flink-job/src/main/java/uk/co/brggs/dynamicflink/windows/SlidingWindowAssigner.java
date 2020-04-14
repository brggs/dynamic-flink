package uk.co.brggs.dynamicflink.windows;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Slf4j
public class SlidingWindowAssigner extends WindowAssigner<WindowDetailProvider, TimeWindow> {

    /**
     * Assign the event to windows starting at the event time, and working back in intervals of the slide amount.
     */
    @Override
    public Collection<TimeWindow> assignWindows(WindowDetailProvider detailProvider, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            val windowSize = detailProvider.getWindowSize() * 1000;
            val windowSlide = detailProvider.getWindowSlide() * 1000;

            List<TimeWindow> windows = new ArrayList<>(windowSize / windowSlide);
            long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, 0, windowSlide);
            for (long start = lastStart;
                 start > timestamp - windowSize;
                 start -= windowSlide) {
                windows.add(new TimeWindow(start, start + windowSize));
            }
            return windows;
        } else {
            log.error("Record has Long.MIN_VALUE timestamp (= no timestamp marker).");
            return Collections.emptyList();
        }
    }

    @Override
    public Trigger<WindowDetailProvider, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new GenericEventTimeTrigger<>();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
