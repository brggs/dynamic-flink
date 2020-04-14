package uk.co.brggs.dynamicflink.rules.conditions;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AllBlocksOccurredRuleCondition implements RuleCondition {
    private List<Integer> blockIndices;

    @Override
    public boolean checkMatch(List<MatchedBlock> blockMatches) {
        return blockIndices.stream().allMatch(i -> blockMatches.stream().anyMatch(b -> b.getMatchedBlockIndex() == i));
    }

    @Override
    public List<String> validate() {
        return IndexValidation.validateIndices(blockIndices);
    }
}
