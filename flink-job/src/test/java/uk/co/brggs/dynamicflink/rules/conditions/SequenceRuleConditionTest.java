package uk.co.brggs.dynamicflink.rules.conditions;

import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequenceRuleConditionTest {

    @Test
    void sequenceSpecified_ShouldBeValid() {
        val condition = new SequenceRuleCondition(
                Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1, true)
                ));
        assertTrue(condition.validate().isEmpty());
    }

    @Test
    void noSequenceSpecified_ShouldBeInvalid() {
        val condition = new SequenceRuleCondition(
                Collections.emptyList());
        assertFalse(condition.validate().isEmpty());
    }

    @Test
    void nullSequenceEntrySpecified_ShouldBeInvalid() {
        val condition = new SequenceRuleCondition(
                Arrays.asList(
                        new SequenceEntry(0),
                        null
                ));
        assertFalse(condition.validate().isEmpty());
    }
}