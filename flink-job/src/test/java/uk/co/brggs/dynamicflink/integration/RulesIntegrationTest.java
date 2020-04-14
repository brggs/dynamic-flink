package uk.co.brggs.dynamicflink.integration;

import uk.co.brggs.dynamicflink.TestEventGenerator;
import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.CompositeCondition;
import uk.co.brggs.dynamicflink.blocks.conditions.CompositeType;
import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import uk.co.brggs.dynamicflink.control.ControlInput;
import uk.co.brggs.dynamicflink.control.ControlInputType;
import uk.co.brggs.dynamicflink.integration.shared.IntegrationTestBase;
import uk.co.brggs.dynamicflink.integration.shared.IntegrationTestCluster;
import uk.co.brggs.dynamicflink.outputevents.OutputEventSerializationSchema;
import uk.co.brggs.dynamicflink.rules.Rule;
import uk.co.brggs.dynamicflink.rules.conditions.*;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RulesIntegrationTest extends IntegrationTestBase {

    @Test
    void ruleOutput_ParsesCorrectlyAsInput() throws Exception {
        val matchingRule = Rule.builder()

                .blocks(Collections.singletonList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build()
                ))
                .baseSeverity(27)
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Collections.singletonList(
                teg.generate(1, "type", "web_server", "check", "one"));

        testCluster.run(controlInput, events);

        val output = new String(new OutputEventSerializationSchema().serialize(
                IntegrationTestCluster.EventSink.values.get(0)));

        val ruleMatchingPreviousOutput = Rule.builder()
                .blocks(Collections.singletonList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("source", "Dynamic_Flink")).build()
                ))
                .baseSeverity(50)
                .build();
        val ruleMatchingPreviousOutputData = new ObjectMapper().writeValueAsString(ruleMatchingPreviousOutput);

        val secondControlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "ruleMatchingPreviousOutput", 1, ruleMatchingPreviousOutputData));

        val secondEvents = Collections.singletonList(output);
        testCluster.run(secondControlInput, secondEvents);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);
        assertThat(IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId()).isEqualTo("ruleMatchingPreviousOutput");
    }

    @Test
    void testRuleWithAndLogic() throws Exception {
        val matchingRule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2)))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .baseSeverity(27)
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val nonMatchingRule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "four")).build()
                ))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2)))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();
        val nonMatchingRuleData = new ObjectMapper().writeValueAsString(nonMatchingRule);

        val controlInput = Arrays.asList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData),
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingRule", 1, nonMatchingRuleData));

        val teg = TestEventGenerator.builder()
                .customer("Test Customer")
                .startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "type", "web_server", "check", "two"),
                teg.generate(1, "type", "web_server", "check", "one"),
                teg.generate(2, "type", "web_server", "check", "two"),
                teg.generate(9, "type", "web_server", "check", "three"));

        testCluster.run(controlInput, events);

        assertEquals(1, IntegrationTestCluster.EventSink.values.size());

        val output = IntegrationTestCluster.EventSink.values.get(0);
        assertThat(output.getCustomer()).isEqualTo("Test Customer");
        assertThat(output.getMatchedRuleId()).isEqualTo("matchingRule");
        assertThat(output.getMatchedRuleVersion()).isEqualTo(1);
        assertThat(output.getGroupBy()).isEqualTo("type=web_server");
        assertThat(output.getSource()).isEqualTo("Dynamic_Flink");
        assertThat(output.getBaseSeverity()).isEqualTo(27);

        assertThat(output.getBlockOutput().size()).isEqualTo(3);
        assertThat(output.getBlockOutput().get("0").getMatchTime()).isEqualTo("2019-02-21T13:00:01.000Z");
        assertThat(output.getBlockOutput().get("2").getMatchTime()).isEqualTo("2019-02-21T13:00:09.000Z");
    }

    @Test
    void testRuleWithOrLogic() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build()
                ))
                .ruleCondition(new AnyBlocksOccurredRuleCondition(Arrays.asList(0, 1)))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val nonMatchingRule = Rule.builder()
                .id("nonMatchingRule")
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new AnyBlocksOccurredRuleCondition(Arrays.asList(0, 1)))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();
        val nonMatchingRuleData = new ObjectMapper().writeValueAsString(nonMatchingRule);

        val controlInput = Arrays.asList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData),
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingRule", 1, nonMatchingRuleData));

        val teg = TestEventGenerator.builder().build();
        val events = Collections.singletonList(
                teg.generate("type", "web_server", "check", "two"));

        testCluster.run(controlInput, events);

        // Event matches multiple times due to the sliding window
        assertEquals(5, IntegrationTestCluster.EventSink.values.size());
        assertEquals("matchingRule", IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId());
    }

    @Test
    void testRuleWithCompositeLogic() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "four")).build()
                ))
                .ruleCondition(
                        new CompositeRuleCondition(
                                CompositeType.OR,
                                Arrays.asList(
                                        new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1)),
                                        new AllBlocksOccurredRuleCondition(Arrays.asList(2, 3))
                                )))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val nonMatchingRule = Rule.builder()
                .id("nonMatchingRule")
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "five")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "six")).build()
                ))
                .ruleCondition(
                        new CompositeRuleCondition(
                                CompositeType.OR,
                                Arrays.asList(
                                        new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1)),
                                        new AllBlocksOccurredRuleCondition(Arrays.asList(2, 3))
                                )))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();
        val nonMatchingRuleData = new ObjectMapper().writeValueAsString(nonMatchingRule);

        val controlInput = Arrays.asList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData),
                new ControlInput(ControlInputType.ADD_RULE, "nonMatchingRule", 1, nonMatchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(1, "type", "web_server", "check", "one"),
                teg.generate(2, "type", "web_server", "check", "three"),
                teg.generate(9, "type", "web_server", "check", "two"));

        testCluster.run(controlInput, events);

        assertEquals(1, IntegrationTestCluster.EventSink.values.size());
        assertEquals("matchingRule", IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId());
    }

    @Test
    void sequenceInCorrectOrder_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1),
                        new SequenceEntry(2))))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(1, "type", "web_server", "check", "one"),
                teg.generate(2, "type", "web_server", "check", "two"),
                teg.generate(9, "type", "web_server", "check", "three"));

        testCluster.run(controlInput, events);

        assertEquals(1, IntegrationTestCluster.EventSink.values.size());
        assertEquals("matchingRule", IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId());
    }

    @Test
    void sequenceInIncorrectOrder_shouldNotProduceAlert() throws Exception {
        val rule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1),
                        new SequenceEntry(2))))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, ruleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(1, "type", "web_server", "check", "one"),
                teg.generate(2, "type", "web_server", "check", "two"),
                teg.generate(9, "type", "web_server", "check", "three"));

        testCluster.run(controlInput, events);

        assertEquals(0, IntegrationTestCluster.EventSink.values.size());
    }

    @Test
    void partialSequenceMatch_shouldNotProduceAlert() throws Exception {
        val rule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1),
                        new SequenceEntry(2))))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "rule", 1, ruleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(1, "type", "web_server", "check", "one"),
                teg.generate(8, "type", "web_server", "check", "two"));

        testCluster.run(controlInput, events);

        assertEquals(0, IntegrationTestCluster.EventSink.values.size());
    }

    @Test
    void ruleWithSequenceIncludingBlockWhichMustNotBePresent_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "four")).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1),
                        new SequenceEntry(2, true))))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(1, "type", "web_server", "check", "one"),
                teg.generate(8, "type", "web_server", "check", "two"),
                teg.generate(9, "type", "web_server", "check", "three"));

        testCluster.run(controlInput, events);

        assertEquals(1, IntegrationTestCluster.EventSink.values.size());
        assertEquals("matchingRule", IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId());
    }

    @Test
    void blockSpecifiedAsNotPresentButIs_shouldNotProduceAlert() throws Exception {
        val rule = Rule.builder()
                .id("rule")
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1),
                        new SequenceEntry(2, true))))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val ruleData = new ObjectMapper().writeValueAsString(rule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "rule", 1, ruleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(1, "type", "web_server", "check", "one"),
                teg.generate(8, "type", "web_server", "check", "two"),
                teg.generate(9, "type", "web_server", "check", "three"));

        testCluster.run(controlInput, events);

        assertEquals(0, IntegrationTestCluster.EventSink.values.size());
    }

    @Test
    void outOfOrderDataMatchingSequence_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .id("matchingRule")
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "three")).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1),
                        new SequenceEntry(2))))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(1, "type", "web_server", "check", "one"),
                teg.generate(9, "type", "web_server", "check", "three"),
                // Late arriving event should be re-ordered
                teg.generate(2, "type", "web_server", "check", "two"));

        testCluster.run(controlInput, events);

        assertEquals(1, IntegrationTestCluster.EventSink.values.size());
        assertEquals("matchingRule", IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId());
    }

    @Test
    void sequenceWithSameGroupByValue_shouldProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder()
                                .type(BlockType.THRESHOLD)
                                .condition(new CompositeCondition(
                                        CompositeType.AND,
                                        Arrays.asList(
                                                new EqualCondition("type", "logon"),
                                                new EqualCondition("status", "failure"))))
                                .windowSize(2)
                                .windowSlide(1)
                                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "3"))
                                .aggregationGroupingFields(Collections.singletonList("remoteIp"))
                                .build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new CompositeCondition(
                                CompositeType.AND,
                                Arrays.asList(
                                        new EqualCondition("type", "logon"),
                                        new EqualCondition("status", "success")))).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1))))
                .groupByField("username")
                .windowSize(3)
                .windowSlide(3)
                .build();

        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1", "username", "1"),
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1", "username", "1"),
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1", "username", "1"),
                teg.generate(1, "type", "logon", "status", "success", "remoteIp", "12.23.45.67", "hostname", "host_1", "username", "1"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(1);
        assertThat(IntegrationTestCluster.EventSink.values.get(0).getMatchedRuleId()).isEqualTo("matchingRule");
    }

    @Test
    void sequenceWithDifferentGroupByValue_shouldNotProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder()
                                .type(BlockType.THRESHOLD)
                                .condition(new CompositeCondition(
                                        CompositeType.AND,
                                        Arrays.asList(
                                                new EqualCondition("type", "logon"),
                                                new EqualCondition("status", "failure"))))
                                .windowSize(2)
                                .windowSlide(1)
                                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "3"))
                                .aggregationGroupingFields(Collections.singletonList("remoteIp"))
                                .build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new CompositeCondition(
                                CompositeType.AND,
                                Arrays.asList(
                                        new EqualCondition("type", "logon"),
                                        new EqualCondition("status", "success")))).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1))))
                .groupByField("username")
                .windowSize(3)
                .windowSlide(3)
                .build();

        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1", "username", "1"),
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1", "username", "1"),
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1", "username", "1"),
                teg.generate(1, "type", "logon", "status", "success", "remoteIp", "12.23.45.67", "hostname", "host_1", "username", "2"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }

    @Test
    void sequenceWithNoGroupByValue_shouldNotProduceAlert() throws Exception {
        val matchingRule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder()
                                .type(BlockType.THRESHOLD)
                                .condition(new CompositeCondition(
                                        CompositeType.AND,
                                        Arrays.asList(
                                                new EqualCondition("type", "logon"),
                                                new EqualCondition("status", "failure"))))
                                .windowSize(2)
                                .windowSlide(1)
                                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "3"))
                                .aggregationGroupingFields(Collections.singletonList("remoteIp"))
                                .build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new CompositeCondition(
                                CompositeType.AND,
                                Arrays.asList(
                                        new EqualCondition("type", "logon"),
                                        new EqualCondition("status", "success")))).build()
                ))
                .ruleCondition(new SequenceRuleCondition(Arrays.asList(
                        new SequenceEntry(0),
                        new SequenceEntry(1))))
                .groupByField("username")
                .windowSize(3)
                .windowSlide(3)
                .build();

        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val teg = TestEventGenerator.builder().startTime(Instant.parse("2019-02-21T13:00:00.000Z")).build();
        val events = Arrays.asList(
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(0, "type", "logon", "status", "failure", "remoteIp", "12.23.45.67", "hostname", "host_1"),
                teg.generate(1, "type", "logon", "status", "success", "remoteIp", "12.23.45.67", "hostname", "host_1"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }

    @Test
    void eventsForDifferentCustomers_AreTreatedSeparately() throws Exception {
        val matchingRule = Rule.builder()
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build()
                ))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1)))
                .groupByField("type")
                .windowSize(10)
                .windowSlide(10)
                .build();
        val matchingRuleData = new ObjectMapper().writeValueAsString(matchingRule);

        val controlInput = Collections.singletonList(
                new ControlInput(ControlInputType.ADD_RULE, "matchingRule", 1, matchingRuleData));

        val customerOneTeg = TestEventGenerator.builder().customer("Two").build();
        val customerTwoTeg = TestEventGenerator.builder().customer("One").build();

        val events = Arrays.asList(
                customerOneTeg.generate(0, "type", "web_server", "check", "two"),
                customerTwoTeg.generate(0, "type", "web_server", "check", "one"));

        testCluster.run(controlInput, events);

        assertThat(IntegrationTestCluster.EventSink.values.size()).isEqualTo(0);
    }
}
