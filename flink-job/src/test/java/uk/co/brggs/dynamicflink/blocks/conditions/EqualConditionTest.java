package uk.co.brggs.dynamicflink.blocks.conditions;

import uk.co.brggs.dynamicflink.TestEventGenerator;
import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EqualConditionTest {

    @Test
    void matchingEvents_ShouldReturnTrue() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val matchingEvent = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "testkey", "abc", "anotherkey", "12"));

        assertTrue(new EqualCondition("testkey", "abc").checkMatch(matchingEvent));
        assertTrue(new EqualCondition("anotherkey", "12").checkMatch(matchingEvent));
    }

    @Test
    void nonMatchingEvents_ShouldReturnFalse() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val nonMatchingEvent = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "testkey", "abc", "anotherkey", "testvalue"));

        assertFalse(new EqualCondition("testkey", "ab").checkMatch(nonMatchingEvent));
    }

    @Test
    void missingField_ShouldReturnFalse() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val nonMatchingEvent = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "anotherkey", "testvalue"));

        assertFalse(new EqualCondition("testkey", "abc").checkMatch(nonMatchingEvent));
    }

    @Test
    void allValuesSpecified_ShouldBeValid() {
        val condition = new EqualCondition("testkey", "value");
        assertTrue(condition.validate().isEmpty());
    }

    @Test
    void emptyKey_ShouldBeInvalid() {
        val condition = new EqualCondition("", "value");
        assertFalse(condition.validate().isEmpty());
    }

    @Test
    void emptyRegex_ShouldBeInvalid() {
        val condition = new EqualCondition("key", "");
        assertFalse(condition.validate().isEmpty());
    }
}