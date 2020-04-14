package uk.co.brggs.dynamicflink.windows;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import uk.co.brggs.dynamicflink.events.AggregatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Emits an alert from a window where the events were flattened into AggregatedEvents.
 */
@Slf4j
public class AggregatedEventProcessWindowFunction extends ProcessWindowFunction<Map<String, AggregatedEvent>, MatchedBlock, Tuple, TimeWindow> {

    /**
     * Emits an alert containing the Aggregated Events.  No conditional rule logic is applied here, this should be
     * done in the preceding trigger function.
     */
    @Override
    public void process(Tuple ruleId, Context ctx, Iterable<Map<String, AggregatedEvent>> elements, Collector<MatchedBlock> out) {
        try {
            // We only get a single element in the iterator, as we are using an aggregate function before this one.
            out.collect(MatchedBlock.createFromAggregatedEvents(elements.iterator().next().values()));
        } catch (Exception ex) {
            log.error("Error processing window for rule {}", ruleId, ex);
        }
    }
}
