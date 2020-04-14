package uk.co.brggs.dynamicflink.blocks;

import uk.co.brggs.dynamicflink.events.AggregatedEvent;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Contains a sample event as {@link String}, and a count indicating how many similar events were seen.
 * Sample event is {@link String} to make it easier for flink to pass it around
 */
@Slf4j
@Data
@Builder
public class InternalEventSummary {
    private String key;
    private String sampleEventContent;
    private int count;

    public static InternalEventSummary createFromAggregatedEvent(AggregatedEvent aggregatedEvent) {
        return InternalEventSummary.builder()
                .key(aggregatedEvent.getSampleEvent().getAggregationKey())
                .sampleEventContent(aggregatedEvent.getSampleEvent().getEventContent())
                .count(aggregatedEvent.getCount())
                .build();
    }
}
