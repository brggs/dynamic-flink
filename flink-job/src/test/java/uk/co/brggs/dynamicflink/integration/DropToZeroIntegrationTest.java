package uk.co.brggs.dynamicflink.integration;

import uk.co.brggs.dynamicflink.TestEventGenerator;
import uk.co.brggs.dynamicflink.blocks.Block;
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

import static org.assertj.core.api.Assertions.assertThat;

class DropToZeroIntegrationTest extends IntegrationTestBase {

    @Test
    void dropToZeroInEventCount_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.DROP_TO_ZERO)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
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
                events.add(teg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "regex"));
            }
        }

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);

        val alert = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(alert.getMatchedRuleId()).isEqualTo("matchingRule");

        val blockOutput = alert.getBlockOutput().get("0");
        assertThat(blockOutput.getMatchMessage()).contains("0 matching events seen");
    }

    @Test
    void insufficientDecreaseInEventCount_shouldNotProduceAlert() throws Exception {
        val nonMatchingRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.DROP_TO_ZERO)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
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
                events.add(teg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "regex"));
            }
        }

        events.add(teg.generate(9 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "regex"));

        testCluster.run(controlInput, events);

        // We'll still get an alert (because the events do stop at the end of the test), but we can check the timestamp
        // to be sure the alert didn't fire too early.
        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);
        assertThat(IntegrationTestCluster.EventSink.values.get(0).getMatchTime()).isEqualTo("2019-02-21T13:11:00.000Z");
    }

    @Test
    void changeToEventsWithDifferingGroupValue_shouldNotProduceAlert() throws Exception {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.DROP_TO_ZERO)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
                        .build()))
                .groupByField("anotherField")
                .build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, ruleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = new ArrayList<String>();

        for (int i = 0; i < 9; i++) {
            for (int j = 0; j < 10; j++) {
                events.add(teg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "regex"));
            }
        }

        for (int i = 0; i < 100; i++) {
            events.add(teg.generate(9 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "anotherValue"));
        }

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);

        val alert = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(alert.getMatchedRuleId()).isEqualTo("matchingRule");
    }

    @Test
    void offsetDropToZeroInEventCount_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.DROP_TO_ZERO)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
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

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);

        val alert = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(alert.getMatchedRuleId()).isEqualTo("matchingRule");

        val blockOutput = alert.getBlockOutput().get("0");
        assertThat(blockOutput.getMatchMessage()).contains("0 matching events seen");
    }

    @Test
    void dropToZeroFollowedByEventForOtherCustomer_shouldProduceAlert() throws Exception {
        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.DROP_TO_ZERO)
                        .condition(new EqualCondition("type", "web_server"))
                        .windowSize(60)
                        .build()))
                .groupByField("hostname")
                .build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, ruleData));

        val customerOneTeg = TestEventGenerator.builder().customer("Two").startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val customerTwoTeg = TestEventGenerator.builder().customer("One").startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = new ArrayList<String>();

        for (int i = 0; i < 9; i++) {
            for (int j = 0; j < 10; j++) {
                events.add(customerOneTeg.generate(i * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));
            }
        }

        events.add(customerTwoTeg.generate(9 * 60, "type", "web_server", "hostname", "importantLaptop", "anotherField", "value"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);

        val alert = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(alert.getMatchedRuleId()).isEqualTo("matchingRule");
        assertThat(IntegrationTestCluster.EventSink.values.get(0).getMatchTime()).isEqualTo("2019-02-21T13:10:00.000Z");
    }
}