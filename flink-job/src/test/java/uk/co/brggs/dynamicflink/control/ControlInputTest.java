package uk.co.brggs.dynamicflink.control;

import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import uk.co.brggs.dynamicflink.rules.Rule;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlInputTest {

    private String validRuleData;

    ControlInputTest() throws JsonProcessingException {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(
                                new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();
        validRuleData = new ObjectMapper().writeValueAsString(rule);
    }

    @Test
    void controlInput_CanBeSerialised() throws IOException {
        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, validRuleData);
        val ciData = new ObjectMapper().writeValueAsString(controlInput);
        val outputControlEvent = new ObjectMapper().readValue(ciData, ControlInput.class);

        assertNotNull(outputControlEvent);
    }

    @Test
    void controlInput_CanBeDeserialised() throws IOException {
        val input = "{\"type\":\"ADD_RULE\",\"ruleId\":\"1\",\"ruleVersion\":1,\"content\":\"{\\\"id\\\":null,\\\"version\\\":0,\\\"blocks\\\":[{\\\"type\\\":\\\"SINGLE_EVENT\\\",\\\"condition\\\":{\\\"@class\\\":\\\"uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition\\\",\\\"key\\\":\\\"hostname\\\",\\\"value\\\":\\\"importantLaptop\\\"},\\\"groupBy\\\":null,\\\"windowSize\\\":0,\\\"windowSlide\\\":0,\\\"aggregationGroupingFields\\\":null,\\\"parameters\\\":null}],\\\"ruleCondition\\\":null,\\\"windowSize\\\":0,\\\"windowSlide\\\":0}\"}";
        val controlInput = new ObjectMapper().readValue(input, ControlInput.class);

        assertNotNull(controlInput);
    }

    @Test
    void controlInputWithValidContent_IsValid() {
        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, validRuleData);
        assertTrue(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithEmptyRuleId_IsInvalid() {
        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "", 1, validRuleData);
        assertFalse(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithInvalidVersion_IsInvalid() {
        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 0, validRuleData);
        assertFalse(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithNullContent_IsInvalid() {
        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, null);
        assertFalse(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithEmptyContent_IsInvalid() {
        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, "");
        assertFalse(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithMatchingRuleIdAndVersion_IsValid() throws JsonProcessingException {
        val rule = Rule.builder()
                .id("ruleId")
                .version(2)
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(
                                new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "ruleId", 2, ruleData);
        assertTrue(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithMismatchedRuleId_IsInvalid() throws JsonProcessingException {
        val rule = Rule.builder()
                .id("anotherId")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(
                                new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, ruleData);
        assertFalse(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithMismatchedRuleVersion_IsInvalid() throws JsonProcessingException {
        val rule = Rule.builder()
                .version(2)
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(
                                new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, ruleData);
        assertFalse(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithInvalidRule_IsInvalid() throws JsonProcessingException {
        val rule = Rule.builder().build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, ruleData);
        assertFalse(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithSchemaVersionOtherThan1_IsInvalid() throws JsonProcessingException {
        val rule = Rule.builder().build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, ruleData);
        controlInput.setControlSchemaVersion(0);
        assertFalse(controlInput.validate().isEmpty());
        controlInput.setControlSchemaVersion(2);
        assertFalse(controlInput.validate().isEmpty());
    }

    @Test
    void controlInputWithSchemaVersion1_IsValid() {
        val controlInput = new ControlInput(ControlInputType.ADD_RULE, "1", 1, validRuleData);
        assertTrue(controlInput.validate().isEmpty());
    }
}
