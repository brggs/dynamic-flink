package uk.co.brggs.dynamicflink.blocks.conditions;

import uk.co.brggs.dynamicflink.TestEventGenerator;
import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComparisonConditionTest {

    @Test
    void matchingEvents_ShouldReturnTrue() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val event = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "testkey", "15", "anotherkey", "testvalue"));

        assertTrue(new ComparisonCondition("testkey", 10, ComparisonType.GREATER_THAN).checkMatch(event));
        assertTrue(new ComparisonCondition("testkey", 10, ComparisonType.GREATER_THAN_OR_EQUAL).checkMatch(event));
        assertTrue(new ComparisonCondition("testkey", 15, ComparisonType.GREATER_THAN_OR_EQUAL).checkMatch(event));
        assertTrue(new ComparisonCondition("testkey", 15, ComparisonType.LESS_THAN_OR_EQUAL).checkMatch(event));
        assertTrue(new ComparisonCondition("testkey", 20, ComparisonType.LESS_THAN_OR_EQUAL).checkMatch(event));
        assertTrue(new ComparisonCondition("testkey", 20, ComparisonType.LESS_THAN).checkMatch(event));
    }

    @Test
    void nonMatchingEvents_ShouldReturnFalse() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val event = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "testkey", "15", "anotherkey", "testvalue"));

        assertFalse(new ComparisonCondition("testkey", 10, ComparisonType.LESS_THAN_OR_EQUAL).checkMatch(event));
        assertFalse(new ComparisonCondition("testkey", 10, ComparisonType.LESS_THAN).checkMatch(event));
        assertFalse(new ComparisonCondition("testkey", 15, ComparisonType.LESS_THAN).checkMatch(event));
        assertFalse(new ComparisonCondition("testkey", 15, ComparisonType.GREATER_THAN).checkMatch(event));
        assertFalse(new ComparisonCondition("testkey", 20, ComparisonType.GREATER_THAN).checkMatch(event));
        assertFalse(new ComparisonCondition("testkey", 20, ComparisonType.GREATER_THAN_OR_EQUAL).checkMatch(event));
    }

    @Test
    void missingField_ShouldReturnFalse() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val event = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "anotherkey", "testvalue"));

        assertFalse(new ComparisonCondition("testkey", 10, ComparisonType.LESS_THAN_OR_EQUAL).checkMatch(event));
    }

    @Test
    void allValuesSpecified_ShouldBeValid() {
        val condition = new ComparisonCondition("testkey", 10, ComparisonType.LESS_THAN_OR_EQUAL);
        assertTrue(condition.validate().isEmpty());
    }

    @Test
    void emptyKey_ShouldBeInvalid() {
        val condition = new ComparisonCondition("", 10, ComparisonType.LESS_THAN_OR_EQUAL);
        assertFalse(condition.validate().isEmpty());
    }
}