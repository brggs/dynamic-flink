package uk.co.brggs.dynamicflink.events;

import uk.co.brggs.dynamicflink.blocks.MatchedEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Contains a sample event, and a count indicating how many similar events were seen.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregatedEvent {
    /**
     * The first event matching the aggregation key for this instance.
     */
    private MatchedEvent sampleEvent;

    /**
     * The timestamp of the latest observed event matching this sample.
     */
    private long latestTimestamp;

    /**
     * The number of events matching this sample.
     */
    private int count = 1;

    public AggregatedEvent(MatchedEvent matchedEvent) {
        sampleEvent = matchedEvent;
        latestTimestamp = matchedEvent.getEventTime();
    }

    /**
     * Register another instance of this event.
     */
    public void update(MatchedEvent matchedEvent) {
        count++;

        if (matchedEvent.getEventTime() > latestTimestamp) {
            latestTimestamp = matchedEvent.getEventTime();
        }
    }
}
