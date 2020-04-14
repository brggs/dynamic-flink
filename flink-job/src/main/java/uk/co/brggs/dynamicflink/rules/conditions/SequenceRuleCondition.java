package uk.co.brggs.dynamicflink.rules.conditions;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Condition which requires a sequence of blocks to be present.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SequenceRuleCondition implements RuleCondition {
    private List<SequenceEntry> conditions;

    @Override
    public boolean checkMatch(List<MatchedBlock> matchedBlocks) {
        var sequenceFound = true;

        matchedBlocks.sort(Comparator.comparing(MatchedBlock::getMatchTime));

        var lastMatchedIndex = 0;
        for (val condition : conditions) {
            var blockFound = false;
            for (int j = lastMatchedIndex; j < matchedBlocks.size(); j++) {
                val currentBlock = matchedBlocks.get(j);
                if (currentBlock.getMatchedBlockIndex() == condition.getBlockIndex()) {
                    // Block found, move on to the next
                    lastMatchedIndex = j;
                    blockFound = true;
                    break;
                }
            }

            if (blockFound && condition.isNotPresent()) {
                // We found a block which should not have appeared in the sequence
                sequenceFound = false;
                break;
            } else if (!blockFound && !condition.isNotPresent()) {
                // We reached the end of the list without finding a match for an expected block
                sequenceFound = false;
                break;
            }
        }

        return sequenceFound;
    }

    @Override
    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (conditions == null || conditions.isEmpty()) {
            validationErrors.add("No conditions specified for SequenceRuleCondition.");
        } else {
            if (conditions.stream().anyMatch(Objects::isNull)) {
                validationErrors.add("Null condition specified for SequenceRuleCondition.");
            }
        }

        return validationErrors;
    }
}

