package uk.co.brggs.dynamicflink.outputevents;

import uk.co.brggs.dynamicflink.blocks.InternalEventSummary;
import uk.co.brggs.dynamicflink.events.AggregatedEvent;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

/**
 * Contains a sample event as deserialized json object and count indicating how many similar events were seen.
 *
 */
@Slf4j
@Data
@AllArgsConstructor
@Builder
public class EventSummary {
    private String key;
    private Any sampleEventContent;
    private int count;

    /**
     * Copies data from {@link InternalEventSummary} and deserialize {@link InternalEventSummary#getSampleEventContent()}
     * @param summary A summary with jsonised sample content
     */
    public EventSummary(InternalEventSummary summary) {
        key = summary.getKey();
        count = summary.getCount();
        sampleEventContent = JsonIterator.deserialize(summary.getSampleEventContent());
    }
}
