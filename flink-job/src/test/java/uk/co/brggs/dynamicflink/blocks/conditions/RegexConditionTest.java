package uk.co.brggs.dynamicflink.blocks.conditions;

import uk.co.brggs.dynamicflink.TestEventGenerator;
import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegexConditionTest {

    @Test
    void matchingEvents_ShouldReturnTrue() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val event = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "testkey", "abc123xyz", "anotherkey", "testvalue"));

        assertTrue(new RegexCondition("testkey", "^ab").checkMatch(event));
        assertTrue(new RegexCondition("testkey", "yz$").checkMatch(event));
        assertTrue(new RegexCondition("testkey", "c12").checkMatch(event));
    }

    @Test
    void nonMatchingEvents_ShouldReturnFalse() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val event = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "testkey", "abc123xyz", "anotherkey", "testvalue"));

        assertFalse(new RegexCondition("testkey", "^yx").checkMatch(event));
        assertFalse(new RegexCondition("testkey", "ab$").checkMatch(event));
    }

    @Test
    void missingField_ShouldReturnFalse() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val event = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "anotherkey", "testvalue"));

        assertFalse(new RegexCondition("testkey", "abc").checkMatch(event));
    }

    @Test
    void allValuesSpecified_ShouldBeValid() {
        val condition = new RegexCondition("testkey", "regex");
        assertTrue(condition.validate().isEmpty());
    }

    @Test
    void emptyKey_ShouldBeInvalid() {
        val condition = new RegexCondition("", "regex");
        assertFalse(condition.validate().isEmpty());
    }

    @Test
    void emptyRegex_ShouldBeInvalid() {
        val condition = new RegexCondition("key", "");
        assertFalse(condition.validate().isEmpty());
    }
}