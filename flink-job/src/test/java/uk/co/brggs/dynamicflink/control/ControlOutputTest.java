package uk.co.brggs.dynamicflink.control;

import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ControlOutputTest {

    @Test
    void controlOutput_CanBeSerialised() throws IOException {
        val input = new ControlInput(ControlInputType.ADD_RULE, "ruleId", 1, "Content");
        val output = new ControlOutput(
                input,
                ControlOutputStatus.RULE_ACTIVE,
                "Rule added");

        val outputString = new ObjectMapper().writeValueAsString(output);
        assertNotNull(outputString);
    }

    @Test
    void controlOutputWithInvalidCharacter_CanBeSerialised() throws IOException {
        val input = new ControlInput(ControlInputType.ADD_RULE, "ruleId", 1, "Content");
        val output = new ControlOutput(
                input,
                ControlOutputStatus.ERROR,
                "Unexpected character ('\"' (code 8220 / 0x201c)): was expecting double-quote to start field name\n" +
                        "at [Source: {\"blocks\":[{\"type\":\"SINGLE_EVENT\",\"condition\":{\"@class\":\"uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition\",\"key\":\"groupBy\",\"value\":\"green\"},\"groupBy\":null,\"windowSize\":0,\"windowSlide\":0,\"aggregationGroupingFields\":null,\"parameters\":null}],\"ruleCondition\":null,\"windowSize\":0,\"windowSlide\":0}; line: 1, column: 3])");

        val outputString = new ObjectMapper().writeValueAsString(output);
        assertNotNull(outputString);
    }
}
