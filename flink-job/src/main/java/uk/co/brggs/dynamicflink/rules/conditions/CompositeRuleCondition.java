package uk.co.brggs.dynamicflink.rules.conditions;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import uk.co.brggs.dynamicflink.blocks.conditions.CompositeType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Condition allowing other conditions to be combined
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompositeRuleCondition implements RuleCondition {
    private CompositeType type;
    private List<RuleCondition> conditions;

    @Override
    public boolean checkMatch(List<MatchedBlock> matchedBlocks) {
        if (type == CompositeType.OR) {
            return conditions.stream().anyMatch(c -> c.checkMatch(matchedBlocks));
        } else {
            return conditions.stream().allMatch(c -> c.checkMatch(matchedBlocks));
        }
    }

    @Override
    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (conditions == null || conditions.isEmpty()) {
            validationErrors.add("No conditions specified for CompositeRuleCondition.");
        } else {
            if (conditions.stream().anyMatch(Objects::isNull)) {
                validationErrors.add("Null condition specified for CompositeRuleCondition.");
            } else {
                conditions.forEach(c -> validationErrors.addAll(c.validate()));
            }
        }

        return validationErrors;
    }
}

