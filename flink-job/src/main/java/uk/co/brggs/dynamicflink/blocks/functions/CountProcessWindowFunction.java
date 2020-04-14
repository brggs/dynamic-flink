package uk.co.brggs.dynamicflink.blocks.functions;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;

/**
 * Adds the window timestamp to the output of a counted window.
 */
@Slf4j
public class CountProcessWindowFunction extends ProcessWindowFunction<Tuple2<MatchedEvent, Long>, Tuple3<Long, MatchedEvent, Long>, Tuple, TimeWindow> {
    @Override
    public void process(Tuple key, Context ctx, Iterable<Tuple2<MatchedEvent, Long>> elements, Collector<Tuple3<Long, MatchedEvent, Long>> out) {
        try {
            // There will only be one element here, as we're aggregating the values in this window
            val element = elements.iterator().next();
            out.collect(new Tuple3<>(ctx.window().getEnd(), element.f0, element.f1));
        } catch (Exception e) {
            log.error("Error collecting count result.", e);
        }
    }
}