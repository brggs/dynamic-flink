package uk.co.brggs.dynamicflink.rules.conditions;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnyBlocksOccurredRuleConditionTest {
    @Test
    void anyBlocksMatched_ShouldReturnTrue() {

        val condition = new AnyBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2));
        val blocks = Collections.singletonList(
                MatchedBlock.builder().matchedBlockIndex(1).build()
        );

        assertTrue(condition.checkMatch(blocks));
    }

    @Test
    void notAllBlocksMatched_ShouldReturnFalse() {

        val condition = new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2));
        List<MatchedBlock> blocks = Collections.emptyList();

        assertFalse(condition.checkMatch(blocks));
    }
}
