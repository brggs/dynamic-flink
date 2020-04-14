package uk.co.brggs.dynamicflink.blocks.threshold;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;

/**
 * Determines when Threshold Block windows should trigger.
 */
@Slf4j
public class ThresholdWindowTrigger extends Trigger<MatchedEvent, TimeWindow> {

    private final ReducingStateDescriptor<Long> countStateDesc =
            new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

    private final ValueStateDescriptor<Boolean> exceededStateDesc =
            new ValueStateDescriptor<>("threshold-exceeded", Types.BOOLEAN);

    @Override
    public TriggerResult onElement(MatchedEvent element, long timestamp, TimeWindow window, TriggerContext ctx) {
        var result = TriggerResult.CONTINUE;

        try {
            val count = ctx.getPartitionedState(countStateDesc);
            count.add(1L);

            val threshold = Integer.parseInt(element.getBlockParameters().get(BlockParameterKey.Threshold));

            // Only trigger when the threshold is first exceeded
            if (count.get() == threshold) {
                val thresholdExceeded = ctx.getPartitionedState(exceededStateDesc);
                thresholdExceeded.update(true);
                result = TriggerResult.FIRE;
            }
        } catch (Exception e) {
            log.error("Error accessing state while evaluating window trigger for element.", e);
        }

        // Ensure the timer is registered, so the window is purged at the max timestamp
        ctx.registerEventTimeTimer(window.maxTimestamp());

        return result;
    }

    /**
     * Purges the window when the max timestamp is reached, and (if the threshold was exceeded) emits a final count.
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        var result = TriggerResult.CONTINUE;

        if (time == window.maxTimestamp()) {
            val thresholdExceeded = ctx.getPartitionedState(exceededStateDesc);
            try {
                if (thresholdExceeded.value() != null && thresholdExceeded.value()) {
                    result = TriggerResult.FIRE_AND_PURGE;
                } else {
                    result = TriggerResult.PURGE;
                }
            } catch (Exception e) {
                log.error("Failed to read thresholdExceeded value", e);
            }
        }

        return result;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) {
            return value1 + value2;
        }

    }
}
