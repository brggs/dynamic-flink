package uk.co.brggs.dynamicflink.blocks.droptozero;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;

import java.util.ArrayList;

/**
 * Compares the final value in the window with the previous windows.  Emits an alert if the final
 * value is zero.
 */
@Slf4j
public class DropToZeroWindowProcessFunction extends ProcessWindowFunction<Tuple3<Long, MatchedEvent, Long>, MatchedBlock, Tuple, TimeWindow> {
    @Override
    public void process(Tuple key, Context ctx, Iterable<Tuple3<Long, MatchedEvent, Long>> elements, Collector<MatchedBlock> out) {
        try {
            val eventCounts = new ArrayList<Tuple3<Long, MatchedEvent, Long>>();
            elements.forEach(eventCounts::add);

            // Only trigger if we have the full set of previous window records
            if (eventCounts.size() == DropToZeroBlockProcessor.ComparisonWindowCount - 1) {
                val lastWindow = eventCounts.get(eventCounts.size() - 1);

                // If the last sub-window we have does not line up with the end of the wider window, then the final
                // sub-window contained 0 events and we need to trigger
                if (lastWindow.f0 != ctx.window().getEnd()) {
                    val matchedEvent = eventCounts.get(0).f1;

                    val message = String.format(
                            "0 matching events seen in the last %d seconds.",
                            matchedEvent.getWindowSize());

                    val matchedBlock = MatchedBlock.populateBuilderFromMatchedEvent(matchedEvent)
                            .matchTime(ctx.window().getEnd())
                            .matchMessage(message)
                            .build();

                    out.collect(matchedBlock);
                }
            }
        } catch (Exception ex) {
            log.error("Error processing drop-to-zero detection window for rule {}", key.getField(0), ex);
        }
    }
}
