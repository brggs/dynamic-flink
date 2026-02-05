package uk.co.brggs.dynamicflink.blocks.uniquethreshold;

import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;
import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;
import java.util.Collections;

/**
 * Determines when Threshold Block windows should trigger.
 */
@Slf4j
public class UniqueThresholdWindowTrigger extends Trigger<MatchedEvent, TimeWindow> {

    private final ListStateDescriptor<String> uniqueValuesStateDesc =
            new ListStateDescriptor<>("unique-values", Types.STRING);

    private final ValueStateDescriptor<Boolean> exceededStateDesc =
            new ValueStateDescriptor<>("threshold-exceeded", Types.BOOLEAN);

    @Override
    public TriggerResult onElement(MatchedEvent element, long timestamp, TimeWindow window, TriggerContext ctx) {
        var result = TriggerResult.CONTINUE;

        val uniqueField = element.getBlockParameters().get(BlockParameterKey.UniqueField);

        try {
            val currentValue = new InputEvent(element.getEventContent()).getField(uniqueField);

            if (currentValue != null && !currentValue.isEmpty()) {
                val uniqueValues = ctx.getPartitionedState(uniqueValuesStateDesc);
                Iterable<String> currentValues = uniqueValues.get();
                
                var count = 0;
                var valueIsNew = true;
                
                if (currentValues != null) {
                    for (val value : currentValues) {
                        count++;
                        if (currentValue.equals(value)) {
                            valueIsNew = false;
                        }
                    }
                }

                if (valueIsNew) {
                    uniqueValues.add(currentValue);
                    count++;
                    
                    val threshold = Integer.parseInt(element.getBlockParameters().get(BlockParameterKey.Threshold));

                    System.out.println("DEBUG: Trigger check: count=" + count + ", threshold=" + threshold + ", value=" + currentValue);

                    // Only trigger when the threshold is first exceeded
                    if (count == threshold) {
                        System.out.println("DEBUG: Trigger FIRING!");
                        val thresholdExceeded = ctx.getPartitionedState(exceededStateDesc);
                        thresholdExceeded.update(true);
                        result = TriggerResult.FIRE;
                    }
                }
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
            } catch (IOException e) {
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
}
