package uk.co.brggs.dynamicflink.blocks;

import uk.co.brggs.dynamicflink.rules.RuleType;
import uk.co.brggs.dynamicflink.rules.conditions.AllBlocksOccurredRuleCondition;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MatchedBlockTest {

    @Test
    void matchingEvents_ShouldReturnTrue() {
        val ruleCondition = new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2));

        val matchedEvent = MatchedEvent.builder()
                .matchedRuleId("ruleId")
                .matchedRuleVersion(5)
                .ruleType(RuleType.COMPLEX)
                .ruleCondition(ruleCondition)
                .matchedBlockIndex(0)
                .ruleWindowSize(10)
                .ruleWindowSlide(1)
                .eventTime(1234)
                .build();

        val matchedBlock = MatchedBlock.createFromMatchedEvent(matchedEvent, "test message");

        assertEquals("test message", matchedBlock.getMatchMessage());
        assertEquals("ruleId", matchedBlock.getMatchedRuleId());
        assertEquals(5, matchedBlock.getMatchedRuleVersion());
        assertEquals(RuleType.COMPLEX, matchedBlock.getRuleType());
        assertEquals(ruleCondition, matchedBlock.getRuleCondition());
        assertEquals(0, matchedBlock.getMatchedBlockIndex());
        assertEquals(10, matchedBlock.getWindowSize());
        assertEquals(1, matchedBlock.getWindowSlide());
        assertEquals(1234, matchedBlock.getMatchTime());
    }
}
