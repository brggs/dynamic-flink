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
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.*;

@Slf4j
class UniqueThresholdIntegrationTest extends IntegrationTestBase {

    @Test
    void thresholdMatched_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.UNIQUE_THRESHOLD)
                        .condition(new EqualCondition("company", "2"))
                        .windowSize(3)
                        .windowSlide(1)
                        .parameters(new HashMap<String, String>() {
                            {
                                put(BlockParameterKey.UniqueField, "username");
                                put(BlockParameterKey.Threshold, "3");
                            }
                        })
                        .aggregationGroupingFields(Collections.singletonList("username"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "company", "1", "username", "user_1", "hostname", "company_1"),
                teg.generate(1, "company", "1", "username", "user_1", "hostname", "company_1"),
                teg.generate(2, "company", "1", "username", "user_1", "hostname", "company_1"),

                teg.generate(3, "company", "2", "username", "user_1", "hostname", "otherhost_1"),
                teg.generate(4, "company", "2", "username", "user_2", "hostname", "otherhost_2"),
                teg.generate(5, "company", "2", "username", "user_3", "hostname", "otherhost_3"),

                teg.generate(3, "company", "2", "username", "user_4", "hostname", "company_2"),
                teg.generate(4, "company", "2", "username", "user_5", "hostname", "company_2"),
                teg.generate(5, "company", "2", "username", "user_6", "hostname", "company_2"));

        testCluster.run(controlInput, events);

        // Two alerts are expected, one for the initial threshold being exceeded, and another at the end of the window.
        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(2);
        assertThat(IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId()).isEqualTo("matchingRule");
    }

    @Test
    void thresholdExceeded_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.UNIQUE_THRESHOLD)
                        .condition(new EqualCondition("company", "2"))
                        .windowSize(10)
                        .windowSlide(10)
                        .parameters(new HashMap<String, String>() {
                            {
                                put(BlockParameterKey.UniqueField, "username");
                                put(BlockParameterKey.Threshold, "3");
                            }
                        })
                        .aggregationGroupingFields(Collections.singletonList("username"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "company", "1", "username", "user_1", "hostname", "company_1"),
                teg.generate(1, "company", "1", "username", "user_1", "hostname", "company_1"),
                teg.generate(2, "company", "1", "username", "user_1", "hostname", "company_1"),

                teg.generate(3, "company", "2", "username", "user_1", "hostname", "otherhost_1"),
                teg.generate(4, "company", "2", "username", "user_2", "hostname", "otherhost_2"),
                teg.generate(5, "company", "2", "username", "user_3", "hostname", "otherhost_3"),

                teg.generate(3, "company", "2", "username", "user_4", "hostname", "company_2"),
                teg.generate(4, "company", "2", "username", "user_5", "hostname", "company_2"),
                teg.generate(4, "company", "2", "username", "user_6", "hostname", "company_2"),
                teg.generate(4, "company", "2", "username", "user_7", "hostname", "company_2"),
                teg.generate(5, "company", "2", "username", "user_8", "hostname", "company_2"));

        testCluster.run(controlInput, events);

        // Two alerts are expected, one for the initial threshold being exceeded, and another at the end of the window.
        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(2);
        assertThat(IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId()).isEqualTo("matchingRule");
    }

    @Test
    void thresholdNotExceeded_shouldNotProduceAlert() throws Exception {
        val nonMatchingRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.UNIQUE_THRESHOLD)
                        .condition(new EqualCondition("company", "1"))
                        .windowSize(3)
                        .windowSlide(1)
                        .parameters(new HashMap<String, String>() {
                            {
                                put(BlockParameterKey.UniqueField, "username");
                                put(BlockParameterKey.Threshold, "3");
                            }
                        })
                        .aggregationGroupingFields(Collections.singletonList("username"))
                        .build()))
                .groupByField("hostname")
                .build();
        val nonMatchingRuleData = new ObjectMapper().writeValueAsString(nonMatchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingRule", 1, nonMatchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "company", "1", "username", "user_1", "hostname", "company_1"),
                teg.generate(1, "company", "1", "username", "user_1", "hostname", "company_1"),
                teg.generate(2, "company", "1", "username", "user_1", "hostname", "company_1"),

                teg.generate(3, "company", "2", "username", "user_1", "hostname", "otherhost_1"),
                teg.generate(4, "company", "2", "username", "user_2", "hostname", "otherhost_2"),
                teg.generate(5, "company", "2", "username", "user_3", "hostname", "otherhost_3"),

                teg.generate(3, "company", "2", "username", "user_4", "hostname", "company_2"),
                teg.generate(4, "company", "2", "username", "user_5", "hostname", "company_2"),
                teg.generate(5, "company", "2", "username", "user_6", "hostname", "company_2"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }

    @Test
    void thresholdExceededWithoutGroupByValue_shouldNotProduceAlert() throws Exception {
        val nonMatchingRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.UNIQUE_THRESHOLD)
                        .condition(new EqualCondition("company", "1"))
                        .windowSize(3)
                        .windowSlide(1)
                        .parameters(new HashMap<String, String>() {
                            {
                                put(BlockParameterKey.UniqueField, "username");
                                put(BlockParameterKey.Threshold, "3");
                            }
                        })
                        .aggregationGroupingFields(Collections.singletonList("username"))
                        .build()))
                .groupByField("hostname")
                .build();
        val nonMatchingRuleData = new ObjectMapper().writeValueAsString(nonMatchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingRule", 1, nonMatchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "company", "1", "username", "user_1"),
                teg.generate(1, "company", "1", "username", "user_2"),
                teg.generate(2, "company", "1", "username", "user_3"),
                teg.generate(2, "company", "1", "username", "user_4"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }

    @Test
    void noMatchingEvents_shouldNotProduceAlert() throws Exception {
        val nonMatchingRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.UNIQUE_THRESHOLD)
                        .condition(new EqualCondition("company", "2"))
                        .windowSize(3)
                        .windowSlide(1)
                        .parameters(new HashMap<String, String>() {
                            {
                                put(BlockParameterKey.UniqueField, "username");
                                put(BlockParameterKey.Threshold, "3");
                            }
                        })
                        .aggregationGroupingFields(Collections.singletonList("username"))
                        .build()))
                .groupByField("hostname")
                .build();
        val nonMatchingRuleData = new ObjectMapper().writeValueAsString(nonMatchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingRule", 1, nonMatchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "company", "1", "username", "user_1"),
                teg.generate(1, "company", "1", "username", "user_2"),
                teg.generate(2, "company", "1", "username", "user_3"),
                teg.generate(2, "company", "1", "username", "user_4"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }

    @Test
    void nullValuesForUniqueField_shouldNotCountTowardsTotal() throws Exception {
        val nonMatchingRule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.UNIQUE_THRESHOLD)
                        .condition(new EqualCondition("company", "1"))
                        .windowSize(3)
                        .windowSlide(3)
                        .parameters(new HashMap<String, String>() {
                            {
                                put(BlockParameterKey.UniqueField, "username");
                                put(BlockParameterKey.Threshold, "3");
                            }
                        })
                        .aggregationGroupingFields(Collections.singletonList("username"))
                        .build()))
                .groupByField("company")
                .build();
        val nonMatchingRuleData = new ObjectMapper().writeValueAsString(nonMatchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingRule", 1, nonMatchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "company", "1"),
                teg.generate(1, "company", "1", "username", "user_0"),
                teg.generate(2, "company", "1", "username", "user_1"),
                teg.generate(2, "company", "1", "username", "user_2"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(2);

        val blockOutput = IntegrationTestCluster.EventSink.values.get(0).getBlockOutput();
        assertThat(blockOutput.get("0").getMatchingEvents().size()).isEqualTo(3);
    }

    @Test
    void thresholdMatchedOverDifferentCustomers_shouldNotProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.UNIQUE_THRESHOLD)
                        .condition(new EqualCondition("company", "2"))
                        .windowSize(3)
                        .windowSlide(1)
                        .parameters(new HashMap<String, String>() {
                            {
                                put(BlockParameterKey.UniqueField, "username");
                                put(BlockParameterKey.Threshold, "3");
                            }
                        })
                        .aggregationGroupingFields(Collections.singletonList("username"))
                        .build()))
                .groupByField("hostname")
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val customerOneTeg = TestEventGenerator.builder().customer("Two").build();
        val customerTwoTeg = TestEventGenerator.builder().customer("One").build();
        val events = Arrays.asList(
                customerOneTeg.generate(0, "company", "2", "username", "user_1", "hostname", "host_1"),
                customerOneTeg.generate(1, "company", "2", "username", "user_2", "hostname", "host_1"),
                customerTwoTeg.generate(2, "company", "2", "username", "user_3", "hostname", "host_1"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }
}
