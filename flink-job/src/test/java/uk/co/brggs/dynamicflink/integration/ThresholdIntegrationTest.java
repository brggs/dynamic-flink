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
import uk.co.brggs.dynamicflink.outputevents.OutputEvent;
import uk.co.brggs.dynamicflink.rules.Rule;
import lombok.val;
import lombok.var;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static uk.co.brggs.dynamicflink.util.jsoniter.AnyAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ThresholdIntegrationTest extends IntegrationTestBase {

    @Test
    void thresholdMatched_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.THRESHOLD)
                        .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                        .windowSize(2)
                        .windowSlide(1)
                        .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                        .aggregationGroupingFields(Collections.singletonList("hostname"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(1, "destinationIp", "12.23.45.67", "hostname", "host_1"));

        testCluster.run(controlInput, events);

        // Two alerts are expected, one for the initial threshold being exceeded, and another at the end of the window.
        assertEquals(2, IntegrationTestCluster.EventSink.values.size());

        val firstOutput = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(firstOutput.getMatchedRuleId()).isEqualTo("matchingRule");
        assertThat(firstOutput.getMatchTime()).isEqualTo("2019-02-21T13:00:01.000Z");
        assertThat(firstOutput.getGroupBy()).isEqualTo("hostname=host_1");
    }

    @Test
    void thresholdExceeded_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.THRESHOLD)
                        .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                        .windowSize(2)
                        .windowSlide(2)
                        .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                        .aggregationGroupingFields(Collections.singletonList("hostname"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(1, "destinationIp", "12.23.45.67", "hostname", "host_1"));

        testCluster.run(controlInput, events);

        // Two alerts are expected, one for the initial threshold being exceeded, and another at the end of the window.
        assertEquals(2, IntegrationTestCluster.EventSink.values.size());
        assertEquals("matchingRule", IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId());
    }

    @Test
    void thresholdNotExceeded_shouldNotProduceAlert() throws Exception {
        val rule = Rule.builder()
                .id("rule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.THRESHOLD)
                        .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                        .windowSize(2)
                        .windowSlide(1)
                        .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "3"))
                        .aggregationGroupingFields(Collections.singletonList("destinationIp"))
                        .build()))
                .groupByField("hostname")
                .build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "rule", 1, ruleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "destinationIp", "12.23.45.67"),
                teg.generate(1, "destinationIp", "12.23.45.67"));

        testCluster.run(controlInput, events);

        assertEquals(0, IntegrationTestCluster.EventSink.values.size());
    }

    @Test
    void thresholdExceededWithoutGroupByField_shouldNotProduceAlert() throws Exception {
        val rule = Rule.builder()
                .id("rule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.THRESHOLD)
                        .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                        .windowSize(2)
                        .windowSlide(1)
                        .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                        .aggregationGroupingFields(Collections.singletonList("destinationIp"))
                        .build()))
                .groupByField("hostname")
                .build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "rule", 1, ruleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "destinationIp", "12.23.45.67"),
                teg.generate(1, "destinationIp", "12.23.45.67"));

        testCluster.run(controlInput, events);

        assertEquals(0, IntegrationTestCluster.EventSink.values.size());
    }

    @Test
    void noMatchingEvents_shouldNotProduceAlert() throws Exception {
        val rule = Rule.builder()
                .id("rule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.THRESHOLD)
                        .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                        .windowSize(2)
                        .windowSlide(1)
                        .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                        .aggregationGroupingFields(Collections.singletonList("other"))
                        .build()))
                .groupByField("hostname")
                .build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "rule", 1, ruleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "destinationIp", "12.23.45.67", "other", "one"),
                teg.generate(1, "destinationIp", "12.23.45.67", "other", "two"));

        testCluster.run(controlInput, events);

        assertEquals(0, IntegrationTestCluster.EventSink.values.size());
    }

    @Test
    void eventsWithDifferingAggregationValues_shouldAppearInSeparateFieldsInAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.THRESHOLD)
                        .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                        .windowSize(2)
                        .windowSlide(1)
                        .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "5"))
                        .aggregationGroupingFields(Collections.singletonList("user"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1", "user", "1"),
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1", "user", "1"),
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1", "user", "1"),
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1", "user", "2"),
                teg.generate(1, "destinationIp", "12.23.45.67", "hostname", "host_1", "user", "2"));

        testCluster.run(controlInput, events);

        List<OutputEvent> outputEvents = IntegrationTestCluster.EventSink.values;
        assertEquals(2, outputEvents.size());
        assertEquals("matchingRule", outputEvents.get(1).getMatchedRuleId());

        val firstBlockOutput = outputEvents.get(0).getBlockOutput();
        assertThat(firstBlockOutput).hasSize(1);

        val firstOutputMatchingEvents = firstBlockOutput.get("0").getMatchingEvents();
        assertThat(firstOutputMatchingEvents).hasSize(2);

        val firstEventSummary = firstOutputMatchingEvents.get(0);
        val secondEventSummary = firstOutputMatchingEvents.get(1);

        assertThat(firstEventSummary.getKey()).isEqualTo("user=1");
        assertThat(firstEventSummary.getSampleEventContent())
                .field("user")
                .isEqualTo("1");
        assertThat(firstEventSummary.getCount()).isEqualTo(3);

        assertThat(secondEventSummary.getKey()).isEqualTo("user=2");
        assertThat(secondEventSummary.getSampleEventContent())
                .field("user")
                .isEqualTo("2");
        assertThat(secondEventSummary.getCount()).isEqualTo(2);
    }

    @Test
    void thresholdSignificantlyExceeded_shouldProduceOnlyTwoAlerts() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.THRESHOLD)
                        .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                        .windowSize(2)
                        .windowSlide(2)
                        .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                        .aggregationGroupingFields(Collections.singletonList("hostname"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(1, "destinationIp", "12.23.45.67", "hostname", "host_1"));

        testCluster.run(controlInput, events);

        // We get an alert for each event that exceeds the threshold
        assertEquals(2, IntegrationTestCluster.EventSink.values.size());
        assertEquals("matchingRule", IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId());

        var matchingEvents = IntegrationTestCluster.EventSink.values
                .get(0)
                .getBlockOutput()
                .get("0")
                .getMatchingEvents();

        // The first alert should contain the first two events
        assertEquals(2, matchingEvents.get(0).getCount());
        assertThat(matchingEvents.get(0).getSampleEventContent()).field("hostname").isEqualTo("host_1");

        matchingEvents = IntegrationTestCluster.EventSink.values
                .get(1)
                .getBlockOutput()
                .get("0")
                .getMatchingEvents();

        // The final alert should contain all matching events
        assertEquals(6, matchingEvents.get(0).getCount());
        assertThat(matchingEvents.get(0).getSampleEventContent()).field("hostname").isEqualTo("host_1");
    }

    @Test
    void thresholdMatchedOverDifferentCustomers_shouldNotProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.THRESHOLD)
                        .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                        .windowSize(2)
                        .windowSlide(1)
                        .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                        .aggregationGroupingFields(Collections.singletonList("hostname"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val customerOneTeg = TestEventGenerator.builder().customer("Two").build();
        val customerTwoTeg = TestEventGenerator.builder().customer("One").build();
        val events = Arrays.asList(
                customerOneTeg.generate(0, "destinationIp", "12.23.45.67", "hostname", "host_1"),
                customerTwoTeg.generate(1, "destinationIp", "12.23.45.67", "hostname", "host_1"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }
}
