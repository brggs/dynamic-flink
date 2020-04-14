package uk.co.brggs.dynamicflink.rules.conditions;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface RuleCondition {
    boolean checkMatch(List<MatchedBlock> blockMatches);

    List<String> validate();
}
