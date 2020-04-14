package uk.co.brggs.dynamicflink.blocks;

import uk.co.brggs.dynamicflink.InputBroadcastProcessFunction;
import uk.co.brggs.dynamicflink.rules.RuleType;
import uk.co.brggs.dynamicflink.rules.conditions.RuleCondition;
import uk.co.brggs.dynamicflink.windows.WindowDetailProvider;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Represents an event which matched a block condition in the {@link InputBroadcastProcessFunction}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MatchedEvent implements WindowDetailProvider {

    /**
     * The unique ID of the customer which generated the event.
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
     * Indicates the type of block which was matched.
     */
    private BlockType blockType;

    /**
     * Specifies the key which should be used to group matching events, which has been extracted using the groupBy
     * defined on the block.  This takes the form "groupBy=value".
     */
    private String groupBy;

    /**
     * The size of the window used to process events within the block, in milliseconds.
     * Does not apply to single event blocks.
     */
    private int windowSize;

    /**
     * The slide of the window used to process events within the block, in milliseconds.
     * Does not apply to single event blocks.
     */
    private int windowSlide;

    /**
     * Block specific parameters.
     */
    private Map<String, String> blockParameters;

    /**
     * If the rule consists of more than one block, the size of the window in which the conditions must be
     * satisfied, in milliseconds.
     */
    private int ruleWindowSize;

    /**
     * If the rule consists of more than one block, the window slide in milliseconds.
     */
    private int ruleWindowSlide;

    /**
     * The time at which the event occurred in UTC.
     */
    private long eventTime;

    /**
     * The original event content.
     */
    private String eventContent;

    /**
     * The key which will be used to combine this event with other similar events in an AggregatedEvent.
     */
    private String aggregationKey;
}
