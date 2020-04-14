package uk.co.brggs.dynamicflink.windows;

import uk.co.brggs.dynamicflink.blocks.MatchedEvent;
import uk.co.brggs.dynamicflink.events.AggregatedEvent;
import lombok.*;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates a list of AggregatedEvents based on the input MatchedEvents.  This contains the event data from the first
 * event with a given key, and a count of how many other events with the same key have been seen.
 */
public class MatchedEventAggregateFunction implements AggregateFunction<MatchedEvent, Map<String, AggregatedEvent>, Map<String, AggregatedEvent>> {
    @Override
    public Map<String, AggregatedEvent> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, AggregatedEvent> add(MatchedEvent event, Map<String, AggregatedEvent> accumulator) {
        val aggregationKey = event.getAggregationKey();

        if (accumulator.containsKey(aggregationKey)) {
            accumulator.get(aggregationKey).update(event);
        } else {
            accumulator.put(aggregationKey, new AggregatedEvent(event));
        }

        return accumulator;
    }

    @Override
    public Map<String, AggregatedEvent> getResult(Map<String, AggregatedEvent> accumulator) {
        return accumulator;
    }

    @Override
    public Map<String, AggregatedEvent> merge(Map<String, AggregatedEvent> a, Map<String, AggregatedEvent> b) {
        b.forEach((key, value) ->
                a.merge(key, value, (v1, v2) ->
                        new AggregatedEvent(
                                v1.getSampleEvent(),
                                v1.getLatestTimestamp() > v2.getLatestTimestamp() ? v1.getLatestTimestamp() : v2.getLatestTimestamp(),
                                v1.getCount() + v2.getCount())));
        return a;
    }
}