package uk.co.brggs.dynamicflink.blocks.simpleanomaly;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;

import java.util.ArrayList;

/**
 * Compares the final value in the window with the average of the previous windows.  Emits an alert if the change
 * is greater than the specified increase factor.
 */
@Slf4j
public class SimpleAnomalyWindowProcessFunction extends ProcessWindowFunction<Tuple3<Long, MatchedEvent, Long>, MatchedBlock, Tuple, TimeWindow> {
    @Override
    public void process(Tuple key, Context ctx, Iterable<Tuple3<Long, MatchedEvent, Long>> elements, Collector<MatchedBlock> out) {
        try {
            val eventCounts = new ArrayList<Tuple3<Long, MatchedEvent, Long>>();
            elements.forEach(eventCounts::add);

            val lastWindow = eventCounts.get(eventCounts.size() - 1);

            // If the last sub-window timestamp doesn't line up with the end of the window, we don't have a count for
            // the last sub-window, which means the count was 0.
            // We also need to have at least 8 of the previous 9 sub-windows (so we'd have 9 in total)
            if ((lastWindow.f0 == ctx.window().getEnd()) && (eventCounts.size() > 8)) {
                // Check whether the last element is significantly larger than the previous average
                var total = 0L;
                for (int i = 0; i < eventCounts.size() - 1; i++) {
                    total += eventCounts.get(i).f2;
                }

                // If we have less than the required number of windows, there have been periods of no activity.
                // As such, we need to divide by ComparisonWindowCount to ensure the average takes into account
                // the periods where the count was 0.
                val previousAverage = total / (SimpleAnomalyBlockProcessor.ComparisonWindowCount - 1);
                val latestCount = lastWindow.f2;

                // Get the parameters from the first MatchedEvent
                val matchedEvent = eventCounts.get(0).f1;
                val params = matchedEvent.getBlockParameters();
                val anomalyThreshold = Integer.parseInt(params.get(BlockParameterKey.SimpleAnomalyThreshold));

                if (latestCount >= previousAverage * anomalyThreshold) {
                    val message = String.format(
                            "%d matching events seen in %d minutes, up from the average previous average of %d.",
                            latestCount,
                            matchedEvent.getWindowSize() / 60,
                            previousAverage);

                    val matchedBlock = MatchedBlock.populateBuilderFromMatchedEvent(matchedEvent)
                            .matchTime(ctx.window().getEnd())
                            .matchMessage(message)
                            .build();

                    out.collect(matchedBlock);
                }
            }
        } catch (Exception ex) {
            log.error("Error processing anomaly detection window for rule {}", key.getField(0), ex);
        }
    }
}
