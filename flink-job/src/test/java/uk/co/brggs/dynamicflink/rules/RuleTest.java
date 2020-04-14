package uk.co.brggs.dynamicflink.rules;

import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.*;
import uk.co.brggs.dynamicflink.rules.conditions.AllBlocksOccurredRuleCondition;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

@Slf4j
class RuleTest {

    @Test
    void simpleRule_CanBeSerialised() throws IOException {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        assertThat(ruleData.contains("importantLaptop")).isTrue();
    }

    @Test
    void simpleRule_CanBeDeserialised() throws IOException {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);
        val outputRule = new ObjectMapper().readValue(ruleData, Rule.class);

        assertThat(outputRule).isNotNull();
    }

    @Test
    void complexRule_CanBeSerialisedAndDeserialised() throws IOException {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new CompositeCondition(
                                CompositeType.AND,
                                Arrays.asList(
                                        new CompositeCondition(
                                                CompositeType.OR,
                                                Arrays.asList(
                                                        new EqualCondition("hostname", "importantLaptop"),
                                                        new ComparisonCondition("testValue", 15, ComparisonType.GREATER_THAN))),
                                        new RegexCondition("destinationIp", "^12\\."))))
                        .build()))
                .build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);
        val outputRule = new ObjectMapper().readValue(ruleData, Rule.class);

        assertThat(outputRule).isNotNull();
    }

    @Test
    void ruleWithSingleBlock_IsValid() {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();

        assertThat(rule.validate()).isEmpty();
    }

    @Test
    void ruleWithMultipleBlocks_IsValid() {
        val rule = Rule.builder()
                .id("matchingRule")
                .version(1)
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2)))
                .groupByField("groupBy")
                .windowSize(10)
                .windowSlide(2)
                .build();

        assertThat(rule.validate()).isEmpty();
    }

    @Test
    void ruleWithSingleBlockAndRuleCondition_IsInvalid() {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("hostname", "importantLaptop"))
                        .build()))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2)))
                .build();

        assertThat(rule.validate()).hasSize(1);
    }

    @Test
    void ruleWithSingleBlockAndWindowParams_IsInvalid() {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("hostname", "importantLaptop"))
                        .build()))
                .windowSize(10)
                .windowSlide(2)
                .build();

        assertThat(rule.validate()).hasSize(2);
    }

    @Test
    void ruleWithSingleEventBlockWithoutGroupBy_IsValid() {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build()
                ))
                .build();

        assertThat(rule.validate()).isEmpty();
    }

    @Test
    void ruleWithSingleThresholdBlockWithoutGroupBy_IsInvalid() {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(
                        Block.builder()
                                .type(BlockType.THRESHOLD)
                                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                                .windowSize(2)
                                .windowSlide(1)
                                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                                .aggregationGroupingFields(Collections.singletonList("aggregationField")).build()
                ))
                .build();

        assertThat(rule.validate()).hasSize(1);
    }

    @Test
    void ruleWithSingleThresholdBlockWithEmptyGroupBy_IsInvalid() {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(
                        Block.builder()
                                .type(BlockType.THRESHOLD)
                                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                                .windowSize(2)
                                .windowSlide(1)
                                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                                .aggregationGroupingFields(Collections.singletonList("aggregationField")).build()
                ))
                .groupByField("")
                .build();

        assertThat(rule.validate()).hasSize(1);
    }

    @Test
    void ruleWithMultipleBlocksWithoutRuleCondition_IsInvalid() {
        val rule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .groupByField("groupBy")
                .windowSize(10)
                .windowSlide(2)
                .build();

        assertThat(rule.validate()).hasSize(1);
    }

    @Test
    void ruleWithMultipleBlocksWithoutWindowParams_IsInvalid() {
        val rule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2)))
                .groupByField("groupBy")
                .build();

        assertThat(rule.validate()).hasSize(2);
    }

    @Test
    void ruleWithMultipleBlocksWithoutGroupBy_IsInvalid() {
        val rule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2)))
                .windowSize(10)
                .windowSlide(2)
                .build();

        assertThat(rule.validate()).hasSize(1);
    }

    @Test
    void ruleWithMultipleBlocksWithEmptyGroupBy_IsInvalid() {
        val rule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2)))
                .groupByField("")
                .windowSize(10)
                .windowSlide(2)
                .build();

        assertThat(rule.validate()).hasSize(1);
    }
}
