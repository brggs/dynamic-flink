package uk.co.brggs.dynamicflink.blocks.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;

/**
 * Counts events, and outputs an example MatchedEvent along with a count of events which matched in the same window.
 */
public class CountAggregateFunction
        implements AggregateFunction<MatchedEvent, Tuple2<MatchedEvent, Long>, Tuple2<MatchedEvent, Long>> {

    @Override
    public Tuple2<MatchedEvent, Long> createAccumulator() {
        return new Tuple2<>(null, 0L);
    }

    @Override
    public Tuple2<MatchedEvent, Long> add(MatchedEvent value, Tuple2<MatchedEvent, Long> accumulator) {
        return new Tuple2<>(value, accumulator.f1 + 1);
    }

    @Override
    public Tuple2<MatchedEvent, Long> getResult(Tuple2<MatchedEvent, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<MatchedEvent, Long> merge(Tuple2<MatchedEvent, Long> a, Tuple2<MatchedEvent, Long> b) {
        return new Tuple2<>(a.f0, a.f1 + b.f1);
    }
}