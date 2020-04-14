package uk.co.brggs.dynamicflink.integration;

import uk.co.brggs.dynamicflink.TestEventGenerator;
import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import uk.co.brggs.dynamicflink.control.ControlInput;
import uk.co.brggs.dynamicflink.control.ControlInputType;
import uk.co.brggs.dynamicflink.integration.shared.IntegrationTestBase;
import uk.co.brggs.dynamicflink.integration.shared.IntegrationTestCluster;
import uk.co.brggs.dynamicflink.rules.Rule;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

class SimpleAnomalyIntegrationTest extends IntegrationTestBase {

    @Test
    void majorIncreaseInEventCount_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SIMPLE_ANOMALY)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
                        .parameters(Collections.singletonMap(BlockParameterKey.SimpleAnomalyThreshold, "10"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = new ArrayList<String>();

        for (int i = 0; i < 9; i++) {
            for (int j = 0; j < 10; j++) {
                events.add(teg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));
            }
        }

        for (int i = 0; i < 100; i++) {
            events.add(teg.generate(9 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "anotherValue"));
        }

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);

        val alert = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(alert.getMatchedRuleId()).isEqualTo("matchingRule");

        val blockOutput = alert.getBlockOutput().get("0");
        assertThat(blockOutput.getMatchMessage()).contains("100 matching events");
    }

    @Test
    void insufficientIncreaseInEventCount_shouldNotProduceAlert() throws Exception {
        val nonMatchingRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SIMPLE_ANOMALY)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
                        .parameters(Collections.singletonMap(BlockParameterKey.SimpleAnomalyThreshold, "12"))
                        .build()))
                .groupByField("hostname")
                .build();
        val nonMatchingRuleData = new ObjectMapper().writeValueAsString(nonMatchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingRule", 1, nonMatchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = new ArrayList<String>();

        for (int i = 0; i < 9; i++) {
            for (int j = 0; j < 10; j++) {
                events.add(teg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));
            }
        }

        for (int i = 0; i < 100; i++) {
            events.add(teg.generate(9 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "anotherValue"));
        }

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }

    @Test
    void majorIncreaseWithDifferingGroupValue_shouldNotProduceAlert() throws Exception {
        val nonMatchingGroupByRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SIMPLE_ANOMALY)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
                        .parameters(Collections.singletonMap(BlockParameterKey.SimpleAnomalyThreshold, "10"))
                        .build()))
                .groupByField("anotherField")
                .build();
        val nonMatchingGroupByRuleData = new ObjectMapper().writeValueAsString(nonMatchingGroupByRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingGroupByRule", 1, nonMatchingGroupByRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = new ArrayList<String>();

        for (int i = 0; i < 9; i++) {
            for (int j = 0; j < 10; j++) {
                events.add(teg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));
            }
        }

        for (int i = 0; i < 100; i++) {
            events.add(teg.generate(9 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "anotherValue"));
        }

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }

    @Test
    void noEventsInSubWindowFollowedByMajorIncrease_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SIMPLE_ANOMALY)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
                        .parameters(Collections.singletonMap(BlockParameterKey.SimpleAnomalyThreshold, "10"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = new ArrayList<String>();

        for (int i = 0; i < 9; i++) {
            for (int j = 0; j < 10; j++) {
                // Skip one set of events to give an empty window
                if (i != 2) {
                    events.add(teg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));
                }
            }
        }

        for (int i = 0; i < 100; i++) {
            events.add(teg.generate(9 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));
        }

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);

        val alert = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(alert.getMatchedRuleId()).isEqualTo("matchingRule");
    }

    @Test
    void offsetMajorIncreaseInEventCount_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SIMPLE_ANOMALY)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
                        .parameters(Collections.singletonMap(BlockParameterKey.SimpleAnomalyThreshold, "10"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = new ArrayList<String>();

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                events.add(teg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));
            }
        }

        for (int i = 0; i < 100; i++) {
            events.add(teg.generate(10 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "anotherValue"));
        }

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);

        val alert = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(alert.getMatchedRuleId()).isEqualTo("matchingRule");

        val blockOutput = alert.getBlockOutput().get("0");
        assertThat(blockOutput.getMatchMessage()).contains("100 matching events");
    }

    @Test
    void majorIncreaseInEventCountForAnotherCustomer_shouldNotProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SIMPLE_ANOMALY)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
                        .parameters(Collections.singletonMap(BlockParameterKey.SimpleAnomalyThreshold, "10"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val customerOneTeg = TestEventGenerator.builder().customer("Two").build();
        val customerTwoTeg = TestEventGenerator.builder().customer("One").build();
        val events = new ArrayList<String>();

        for (int i = 0; i < 9; i++) {
            for (int j = 0; j < 10; j++) {
                events.add(customerOneTeg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));
            }
        }

        for (int i = 0; i < 100; i++) {
            events.add(customerTwoTeg.generate(9 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "anotherValue"));
        }

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }
}