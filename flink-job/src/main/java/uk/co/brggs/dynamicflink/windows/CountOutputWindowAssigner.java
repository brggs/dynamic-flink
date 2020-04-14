package uk.co.brggs.dynamicflink.windows;

import uk.co.brggs.dynamicflink.blocks.MatchedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Assigns the output from windows using the CountAggregationFunction to a further, larger window.
 */
@RequiredArgsConstructor
@Slf4j
public class CountOutputWindowAssigner extends WindowAssigner<Tuple3<Long, MatchedEvent, Long>, TimeWindow> {
    private final int comparisonWindowCount;

    /**
     * Assign the event to windows starting at the event time, and working back in intervals of the slide amount.
     */
    @Override
    public Collection<TimeWindow> assignWindows(Tuple3<Long, MatchedEvent, Long> blockMatchCount, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            val ruleWindowSize = blockMatchCount.f1.getWindowSize() * 1000;
            val comparisonWindowSize = ruleWindowSize * comparisonWindowCount;

            List<TimeWindow> windows = new ArrayList<>(comparisonWindowSize / ruleWindowSize);
            long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, 0, ruleWindowSize);
            for (long start = lastStart;
                 start > timestamp - comparisonWindowSize;
                 start -= ruleWindowSize) {
                windows.add(new TimeWindow(start, start + comparisonWindowSize));
            }
            return windows;
        } else {
            log.error("Record has Long.MIN_VALUE timestamp (= no timestamp marker).");
            return Collections.emptyList();
        }
    }

    @Override
    public Trigger<Tuple3<Long, MatchedEvent, Long>, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
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
