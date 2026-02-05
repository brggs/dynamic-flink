package uk.co.brggs.dynamicflink.blocks;

import uk.co.brggs.dynamicflink.events.AggregatedEvent;
import uk.co.brggs.dynamicflink.rules.RuleType;
import uk.co.brggs.dynamicflink.rules.conditions.RuleCondition;
import uk.co.brggs.dynamicflink.windows.WindowDetailProvider;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents the output of a block which has triggered.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MatchedBlock implements WindowDetailProvider {

    /**
     * The unique ID of the customer which generated the matching events.
     */
    private String customer;

    /**
     * The unique ID of the rule containing the matching block.
     */
    private String matchedRuleId;

    /**
     * The version number of the rule containing the matching block.
     */
    private int matchedRuleVersion;

    /**
     * The base severity of the rule.
     */
    private int baseSeverity;

    /**
     * Indicates the type of rule.
     */
    private RuleType ruleType;

    /**
     * The conditions which must be met in order for the rule to generate an output.
     */
    private RuleCondition ruleCondition;

    /**
     * The index of the matched block in the rule block list.
     */
    private int matchedBlockIndex;

    /**
     * A timestamp indicating when the block match occurred.
     */
    private long matchTime;

    /**
     * Specifies the key which should be used to group matching events, which has been extracted using the groupBy
     * defined on the block.  This takes the form "groupBy=value".
     */
    private String groupBy;

    /**
     * If the rule consists of more than one block, the size of the window in which the conditions must be
     * satisfied, in milliseconds.
     */
    private int windowSize;

    /**
     * If the rule consists of more than one block, the window slide in milliseconds.
     */
    private int windowSlide;

    /**
     * The events which matched the block, collapsed into AggregatedEvents.
     */
    private List<InternalEventSummary> matchingEvents;

    /**
     * A description of why the block matched.
     */
    private String matchMessage;

    /**
     * Creates a MatchedBlock from a MatchedEvent, with a message, using the timestamp of the event.
     */
    public static MatchedBlock createFromMatchedEvent(MatchedEvent matchedEvent, String matchMessage) {
        return populateBuilderFromMatchedEvent(matchedEvent)
                .matchTime(matchedEvent.getEventTime())
                .matchMessage(matchMessage)
                .matchingEvents(Collections.singletonList(
                        InternalEventSummary.builder()
                                .sampleEventContent(matchedEvent.getEventContent())
                                .count(1)
                                .build()))
                .build();
    }

    /**
     * Creates a MatchedBlock from a list of AggregatedEvents, using the timestamp specified.
     */
    public static MatchedBlock createFromAggregatedEvents(Collection<AggregatedEvent> aggregatedEvents) {
        val eventSummaries = new ArrayList<InternalEventSummary>();
        var latestTimestamp = 0L;

        for (var e : aggregatedEvents) {
            eventSummaries.add(InternalEventSummary.createFromAggregatedEvent(e));
            if (e.getLatestTimestamp() > latestTimestamp) {
                latestTimestamp = e.getLatestTimestamp();
            }
        }

        val sampleEvent = aggregatedEvents.iterator().next().getSampleEvent();

        return populateBuilderFromMatchedEvent(sampleEvent)
                .matchTime(latestTimestamp)
                .matchingEvents(eventSummaries)
                .build();
    }

    public static MatchedBlock.MatchedBlockBuilder populateBuilderFromMatchedEvent(MatchedEvent matchedEvent) {
        return MatchedBlock.builder()
                .customer(matchedEvent.getCustomer())
                .matchedRuleId(matchedEvent.getMatchedRuleId())
                .matchedRuleVersion(matchedEvent.getMatchedRuleVersion())
                .baseSeverity(matchedEvent.getBaseSeverity())
                .ruleType(matchedEvent.getRuleType())
                .ruleCondition(matchedEvent.getRuleCondition())
                .matchedBlockIndex(matchedEvent.getMatchedBlockIndex())
                .groupBy(matchedEvent.getGroupBy())
                .windowSize(matchedEvent.getRuleWindowSize())
                .windowSlide(matchedEvent.getRuleWindowSlide());
    }
}
