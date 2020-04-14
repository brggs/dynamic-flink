package uk.co.brggs.dynamicflink.rules.conditions;

import uk.co.brggs.dynamicflink.blocks.conditions.CompositeType;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompositeRuleConditionTest {

    @Test
    void conditionsSpecified_ShouldBeValid() {
        val condition = new CompositeRuleCondition(
                CompositeType.AND,
                Arrays.asList(
                        new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1)),
                        new AllBlocksOccurredRuleCondition(Arrays.asList(2, 3))
                ));
        assertTrue(condition.validate().isEmpty());
    }

    @Test
    void noConditionsSpecified_ShouldBeInvalid() {
        val condition = new CompositeRuleCondition(
                CompositeType.AND,
                Collections.emptyList());
        assertFalse(condition.validate().isEmpty());
    }

    @Test
    void nullConditionSpecified_ShouldBeInvalid() {
        val condition = new CompositeRuleCondition(
                CompositeType.AND,
                Arrays.asList(
                        new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1)),
                        null
                ));
        assertFalse(condition.validate().isEmpty());
    }

    @Test
    void invalidSubConditionSpecified_ShouldBeInvalid() {
        val condition = new CompositeRuleCondition(
                CompositeType.AND,
                Arrays.asList(
                        new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1)),
                        new AllBlocksOccurredRuleCondition(null)
                ));
        assertFalse(condition.validate().isEmpty());
    }
}