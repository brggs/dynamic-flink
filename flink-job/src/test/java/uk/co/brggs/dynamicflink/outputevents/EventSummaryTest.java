package uk.co.brggs.dynamicflink.outputevents;

import uk.co.brggs.dynamicflink.blocks.InternalEventSummary;
import lombok.val;
import org.junit.jupiter.api.Test;

import static uk.co.brggs.dynamicflink.util.jsoniter.AnyAssert.assertThat;

class EventSummaryTest {
    @Test
    void canBeCreatedFromSerializedEventSummary() {

        val content = "{\"name\": \"name of the event\", \"count\": 2}";

        val serializedSummary = InternalEventSummary.builder()
                .count(2)
                .sampleEventContent(content)
                .key("theKey")
                .build();

        val summary = new EventSummary(serializedSummary);

        assertThat(summary.getSampleEventContent())
                .field("name")
                .isEqualTo("name of the event");

        assertThat(summary.getSampleEventContent())
                .field("count")
                .isEqualTo(2L);
    }
}
