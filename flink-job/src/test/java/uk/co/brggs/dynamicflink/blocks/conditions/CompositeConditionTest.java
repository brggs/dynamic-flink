package uk.co.brggs.dynamicflink.blocks.conditions;

import uk.co.brggs.dynamicflink.TestEventGenerator;
import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompositeConditionTest {

    @Test
    void matchingEvents_ShouldReturnTrue() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val event = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "testkey", "abc", "anotherkey", "xyz"));

        assertTrue(new CompositeCondition(
                CompositeType.AND,
                Arrays.asList(
                        new EqualCondition("testkey", "abc"),
                        new EqualCondition("anotherkey", "xyz")
                )).checkMatch(event));


        assertTrue(new CompositeCondition(
                CompositeType.OR,
                Arrays.asList(
                        new EqualCondition("testkey", "abc"),
                        new EqualCondition("nonexistent", "xyz")
                )).checkMatch(event));
    }

    @Test
    void nonMatchingEvents_ShouldReturnFalse() throws IOException {
        val teg = TestEventGenerator.builder().build();
        val event = new InputEvent(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop", "testkey", "abc123xyz", "anotherkey", "testvalue"));

        assertFalse(new CompositeCondition(
                CompositeType.AND,
                Arrays.asList(
                        new EqualCondition("testkey", "abc"),
                        new EqualCondition("nonexistent", "xyz")
                )).checkMatch(event));


        assertFalse(new CompositeCondition(
                CompositeType.OR,
                Arrays.asList(
                        new EqualCondition("nonexistent1", "abc"),
                        new EqualCondition("nonexistent2", "xyz")
                )).checkMatch(event));
    }

    @Test
    void conditionsSpecified_ShouldBeValid() {
        val condition = new CompositeCondition(
                CompositeType.AND,
                Arrays.asList(
                        new EqualCondition("testkey", "abc"),
                        new EqualCondition("nonexistent", "xyz")
                ));
        assertTrue(condition.validate().isEmpty());
    }

    @Test
    void noConditionsSpecified_ShouldBeInvalid() {
        val condition = new CompositeCondition(
                CompositeType.AND,
                Collections.emptyList());
        assertFalse(condition.validate().isEmpty());
    }

    @Test
    void nullConditionSpecified_ShouldBeInvalid() {
        val condition = new CompositeCondition(
                CompositeType.AND,
                Arrays.asList(
                        new EqualCondition("testkey", "abc"),
                        null
                ));
        assertFalse(condition.validate().isEmpty());
    }

    @Test
    void invalidSubConditionSpecified_ShouldBeInvalid() {
        val condition = new CompositeCondition(
                CompositeType.AND,
                Collections.singletonList(
                        new EqualCondition("", "abc")
                ));
        assertFalse(condition.validate().isEmpty());
    }
}