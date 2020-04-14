package uk.co.brggs.dynamicflink.rules;

import lombok.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.rules.conditions.RuleCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a rule containing one or more blocks.  These define which events will be matched.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Rule {

    /**
     * The unique ID of the rule.
     */
    private String id;

    /**
     * The version number of the rule.
     */
    private int version;

    /**
     * The base severity of events produced by this rule, before customer specific modifiers are applied.
     */
    private int baseSeverity;

    /**
     * The blocks which make up the rule.
     */
    private List<Block> blocks;

    /**
     * For rules containing more than one block, the condition specifies how they should be combined in order for the
     * rule to produce an output.  E.g. All blocks required.
     */
    private RuleCondition ruleCondition;

    /**
     * The field which should be used for grouping events within the rule.  Events which contain the same value in this
     * field will be grouped together both within block and within the rule as a whole.
     */
    private String groupByField;

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
     * Validates the content of the rule.
     *
     * @return A list of validation errors.  An empty list if the object is valid.
     */
    @JsonIgnore
    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (blocks == null || blocks.isEmpty()) {
            validationErrors.add("Rule did not contain any Blocks.");
        } else {
            blocks.forEach(b -> validationErrors.addAll(b.validate()));

            if (blocks.size() > 1) {
                if (ruleCondition == null) {
                    validationErrors.add("Rule with multiple blocks must include a rule condition.");
                } else {
                    validationErrors.addAll(ruleCondition.validate());
                }

                if (windowSize <= 0) {
                    validationErrors.add("Rule with multiple blocks must include a window size.");
                }
                if (windowSlide <= 0) {
                    validationErrors.add("Rule with multiple blocks must include a window slide.");
                }
            } else {
                if (ruleCondition != null) {
                    validationErrors.add("Rule with single block must not include a rule condition.");
                }
                if (windowSize > 0) {
                    validationErrors.add("Rule with single blocks must not include a window size.");
                }
                if (windowSlide > 0) {
                    validationErrors.add("Rule with single blocks must not include a window slide.");
                }
            }
        }

        // groupBy must be set unless the rule consists of only one SingleEvent block.
        if (blocks.size() > 1 || blocks.get(0).getType() != BlockType.SINGLE_EVENT) {
            if (groupByField == null || groupByField.isEmpty()) {
                validationErrors.add("GroupByField was not set for rule.");
            }
        }

        return validationErrors;
    }
}
