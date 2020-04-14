package uk.co.brggs.dynamicflink.rules.conditions;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AllBlocksOccurredRuleConditionTest {
    @Test
    void allBlocksMatched_ShouldReturnTrue() {

        val condition = new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2));
        val blocks = Arrays.asList(
                MatchedBlock.builder().matchedBlockIndex(0).build(),
                MatchedBlock.builder().matchedBlockIndex(1).build(),
                MatchedBlock.builder().matchedBlockIndex(2).build()
        );

        assertTrue(condition.checkMatch(blocks));
    }

    @Test
    void notAllBlocksMatched_ShouldReturnFalse() {

        val condition = new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2));
        val blocks = Arrays.asList(
                MatchedBlock.builder().matchedBlockIndex(0).build(),
                MatchedBlock.builder().matchedBlockIndex(1).build()
        );

        assertFalse(condition.checkMatch(blocks));
    }

    @Test
    void indicesSpecified_ShouldBeValid() {
        val condition = new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2));
        assertTrue(condition.validate().isEmpty());
    }
}
