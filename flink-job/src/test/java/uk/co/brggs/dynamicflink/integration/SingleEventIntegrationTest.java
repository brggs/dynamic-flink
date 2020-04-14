package uk.co.brggs.dynamicflink.integration;

import uk.co.brggs.dynamicflink.TestEventGenerator;
import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.*;
import uk.co.brggs.dynamicflink.control.ControlInput;
import uk.co.brggs.dynamicflink.control.ControlInputType;
import uk.co.brggs.dynamicflink.integration.shared.IntegrationTestBase;
import uk.co.brggs.dynamicflink.integration.shared.IntegrationTestCluster;
import uk.co.brggs.dynamicflink.rules.Rule;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.co.brggs.dynamicflink.util.jsoniter.AnyAssert.assertThat;

class SingleEventIntegrationTest extends IntegrationTestBase {

    @Test
    void simpleMatchingCondition_shouldProduceAlert() throws Exception {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(
                                new CompositeCondition(
                                        CompositeType.AND,
                                        Arrays.asList(
                                                new EqualCondition("hostname", "importantLaptop"),
                                                new EqualCondition("destinationIp", "12.23.45.67"))))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);
        val controlInput = Collections.singletonList(new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, ruleData));

        val teg = TestEventGenerator.builder()
                .startTime(Instant.parse("2019-05-21T12:00:00.000Z"))
                .build();

        val matchingEventContent = teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop");

        val events = Arrays.asList(
                matchingEventContent,
                teg.generate("destinationIp", "12.23.45.67", "hostname", "anotherLaptop"));

        testCluster.run(controlInput, events);

        assertEquals(1, IntegrationTestCluster.EventSink.values.size());

        val outputEvent = IntegrationTestCluster.EventSink.values.get(0);
        assertEquals("matchingRule", outputEvent.getMatchedRuleId());
        assertEquals(1, outputEvent.getMatchedRuleVersion());
        assertEquals("2019-05-21T12:00:00.000Z", outputEvent.getMatchTime());
        val matchedEvents = outputEvent.getBlockOutput().get("0").getMatchingEvents();
        assertEquals(1, matchedEvents.size());
        assertEquals(1, matchedEvents.get(0).getCount());
        assertEquals(4, matchedEvents.get(0).getSampleEventContent().keys().size());
        assertThat(matchedEvents.get(0).getSampleEventContent())
                .field("destinationIp")
                .isEqualTo("12.23.45.67");
        assertThat(matchedEvents.get(0).getSampleEventContent())
                .field("hostname")
                .isEqualTo("importantLaptop");
        assertThat(matchedEvents.get(0).getSampleEventContent())
                .field("customer")
                .isEqualTo("CustomerOne");
        assertThat(matchedEvents.get(0).getSampleEventContent())
                .field("@timestamp")
                .isEqualTo("2019-05-21T12:00:00Z");
    }

    @Test
    void simpleNonMatchingCondition_shouldNotProduceAlert() throws Exception {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("hostname", "importantLaptop"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);
        val controlInput = Collections.singletonList(new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, ruleData));

        val teg = TestEventGenerator.builder().build();
        val events = Collections.singletonList(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "anotherLaptop"));

        testCluster.run(controlInput, events);

        assertEquals(0, IntegrationTestCluster.EventSink.values.size());
    }

    @Test
    void complexMatchingCondition_shouldProduceAlert() throws Exception {
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
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);
        val controlInput = Collections.singletonList(new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, ruleData));

        val teg = TestEventGenerator.builder().build();
        val events = Arrays.asList(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "anotherLaptop", "testValue", "20"),
                teg.generate("destinationIp", "12.23.45.67", "hostname", "importantLaptop"),
                teg.generate("destinationIp", "22.23.45.67", "hostname", "anotherLaptop", "testValue", "20"),
                teg.generate("destinationIp", "22.23.45.67", "hostname", "importantLaptop"));

        testCluster.run(controlInput, events);

        assertEquals(2, IntegrationTestCluster.EventSink.values.size());
        assertEquals("matchingRule", IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId());
    }

    @Test
    void complexNonMatchingCondition_shouldNotProduceAlert() throws Exception {
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
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);
        val controlInput = Collections.singletonList(new ControlInput(ControlInputType.ADD_RULE, "1", 1, ruleData));

        val teg = TestEventGenerator.builder().build();
        val events = Arrays.asList(
                teg.generate("destinationIp", "12.23.45.67", "hostname", "anotherLaptop", "testValue", "2"),
                teg.generate("destinationIp", "12.23.45.67", "hostname", "anotherLaptop"),
                teg.generate("destinationIp", "22.23.45.67", "hostname", "anotherLaptop", "testValue", "20"),
                teg.generate("destinationIp", "22.23.45.67", "hostname", "importantLaptop"));

        testCluster.run(controlInput, events);

        assertEquals(0, IntegrationTestCluster.EventSink.values.size());
    }
}