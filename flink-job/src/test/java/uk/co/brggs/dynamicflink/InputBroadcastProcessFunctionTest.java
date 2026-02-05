package uk.co.brggs.dynamicflink;

import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;
import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import uk.co.brggs.dynamicflink.control.ControlInput;
import uk.co.brggs.dynamicflink.control.ControlInputType;
import uk.co.brggs.dynamicflink.control.ControlOutput;
import uk.co.brggs.dynamicflink.control.ControlOutputStatus;
import uk.co.brggs.dynamicflink.rules.Rule;
import uk.co.brggs.dynamicflink.rules.RuleType;
import uk.co.brggs.dynamicflink.rules.conditions.AllBlocksOccurredRuleCondition;
import lombok.val;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

// Suppress warnings caused by mocking generics with Mockito.
@SuppressWarnings({"unchecked"})
class InputBroadcastProcessFunctionTest {

    @Test
    void newRule_shouldAddRule() throws Exception {
        val ruleId = "ruleId";

        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("processName", "process.exe"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        BroadcastState<String, Rule> ruleState = Mockito.mock(BroadcastState.class);
        val contextMock = Mockito.mock(InputBroadcastProcessFunction.Context.class);
        Mockito.<BroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val testFunction = new InputBroadcastProcessFunction();
        testFunction.processBroadcastElement(new ControlInput(ControlInputType.ADD_RULE, ruleId, 1, ruleData), contextMock, null);

        val ruleCaptor = ArgumentCaptor.forClass(Rule.class);
        verify(ruleState, times(1)).put(any(String.class), ruleCaptor.capture());
        val capturedRules = ruleCaptor.getAllValues();
        assertEquals(ruleId, capturedRules.get(0).getId());

        val outputCaptor = ArgumentCaptor.forClass(ControlOutput.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());
        val capturedOutput = outputCaptor.getAllValues();
        assertEquals(ruleId, capturedOutput.get(0).getRuleId());
        assertEquals(ControlOutputStatus.RULE_ACTIVE, capturedOutput.get(0).getStatus());
    }

    @Test
    void newVersionOfRule_shouldUpdateRule() throws Exception {
        val ruleId = "ruleId";

        val rule = Rule.builder()
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("processName", "process.exe"))
                        .build())).build();
        val ruleData = new ObjectMapper().writeValueAsString(rule);

        BroadcastState<String, Rule> ruleState = Mockito.mock(BroadcastState.class);
        when(ruleState.contains(ruleId)).thenReturn(true);
        when(ruleState.get(ruleId)).thenReturn(rule);

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.Context.class);
        Mockito.<BroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val testFunction = new InputBroadcastProcessFunction();
        testFunction.processBroadcastElement(new ControlInput(ControlInputType.ADD_RULE, ruleId, 2, ruleData), contextMock, null);

        val ruleCaptor = ArgumentCaptor.forClass(Rule.class);
        verify(ruleState, times(1)).put(any(String.class), ruleCaptor.capture());
        val capturedRules = ruleCaptor.getAllValues();
        assertEquals(ruleId, capturedRules.get(0).getId());

        val outputCaptor = ArgumentCaptor.forClass(ControlOutput.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());
        val capturedOutput = outputCaptor.getAllValues();
        assertEquals(ruleId, capturedOutput.get(0).getRuleId());
        assertEquals(ControlOutputStatus.RULE_ACTIVE, capturedOutput.get(0).getStatus());
    }

    @Test
    void oldVersionOfRule_shouldNotUpdateRule() throws Exception {
        val ruleId = "ruleId";
        val rule = Rule.builder()
                .version(1)
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("processName", "process.exe"))
                        .build())).build();

        val oldRuleData = new ObjectMapper().writeValueAsString(rule);

        rule.setVersion(2);

        BroadcastState<String, Rule> ruleState = Mockito.mock(BroadcastState.class);
        when(ruleState.contains(ruleId)).thenReturn(true);
        when(ruleState.get(ruleId)).thenReturn(rule);

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.Context.class);
        Mockito.<BroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val testFunction = new InputBroadcastProcessFunction();

        testFunction.processBroadcastElement(new ControlInput(ControlInputType.ADD_RULE, ruleId, 1, oldRuleData), contextMock, null);

        verify(ruleState, never()).put(any(String.class), any(Rule.class));

        val outputCaptor = ArgumentCaptor.forClass(ControlOutput.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());
        val capturedOutput = outputCaptor.getAllValues();
        assertEquals(ruleId, capturedOutput.get(0).getRuleId());
        assertEquals(2, capturedOutput.get(0).getRuleVersion());
        assertEquals(ControlOutputStatus.RULE_ACTIVE, capturedOutput.get(0).getStatus());
    }

    @Test
    void removeRule_shouldRemoveRule() throws Exception {
        val ruleId = "ruleId";
        val rule = Rule.builder()
                .id(ruleId)
                .version(2)
                .build();

        BroadcastState<String, Rule> ruleState = Mockito.mock(BroadcastState.class);
        when(ruleState.contains(ruleId)).thenReturn(true);
        when(ruleState.get(ruleId)).thenReturn(rule);

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.Context.class);
        Mockito.<BroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val testFunction = new InputBroadcastProcessFunction();

        testFunction.processBroadcastElement(new ControlInput(ControlInputType.REMOVE_RULE, ruleId, 2, null), contextMock, null);

        verify(ruleState, times(1)).remove(ruleId);

        val outputCaptor = ArgumentCaptor.forClass(ControlOutput.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());
        val capturedOutput = outputCaptor.getAllValues();
        assertEquals(ruleId, capturedOutput.get(0).getRuleId());
        assertEquals(ControlOutputStatus.RULE_DEACTIVATED, capturedOutput.get(0).getStatus());
    }

    @Test
    void queryStatus_shouldReturnRules() throws Exception {
        val ruleId = "ruleId";
        val rule = Rule.builder()
                .id(ruleId)
                .version(2)
                .build();

        BroadcastState<String, Rule> ruleState = Mockito.mock(BroadcastState.class);
        when(ruleState.entries()).thenReturn(Collections.singletonMap(ruleId, rule).entrySet());

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.Context.class);
        Mockito.<BroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val testFunction = new InputBroadcastProcessFunction();

        testFunction.processBroadcastElement(new ControlInput(ControlInputType.QUERY_STATUS), contextMock, null);

        val outputCaptor = ArgumentCaptor.forClass(ControlOutput.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());
        val capturedOutput = outputCaptor.getAllValues();
        assertThat(capturedOutput.get(0).getStatus()).isEqualTo(ControlOutputStatus.STATUS_UPDATE);
    }

    @Test
    void eventMatchingSimpleRule_ShouldBeSentToOutput() throws Exception {
        val ruleId = "ruleId";

        val rule = Rule.builder()
                .id(ruleId)
                .version(2)
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("processName", "process.exe"))
                        .build())).build();

        val map = Collections.singletonMap(ruleId, rule);
        ReadOnlyBroadcastState<String, Rule> ruleState = Mockito.mock(ReadOnlyBroadcastState.class);
        when(ruleState.immutableEntries()).thenReturn(map.entrySet());

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.ReadOnlyContext.class);
        Mockito.<ReadOnlyBroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);
        when(contextMock.timestamp()).thenReturn(1234L);

        val teg = TestEventGenerator.builder().build();
        val matchingEvent = teg.generate("sourceHostName", "lavender", "processName", "process.exe");

        val testFunction = new InputBroadcastProcessFunction();
        Collector<MatchedEvent> collector = Mockito.mock(Collector.class);
        testFunction.processElement(matchingEvent, contextMock, collector);

        val outputCaptor = ArgumentCaptor.forClass(MatchedEvent.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());
        verify(collector, times(1)).collect(any(MatchedEvent.class));

        val matchedEvent = outputCaptor.getAllValues().get(0);
        assertEquals(ruleId, matchedEvent.getMatchedRuleId());
        assertEquals(2, matchedEvent.getMatchedRuleVersion());
        assertEquals(RuleType.SIMPLE, matchedEvent.getRuleType());
        assertNull(matchedEvent.getRuleCondition()); // Null, as it's only a simple rule
        assertEquals(0, matchedEvent.getMatchedBlockIndex());
        assertEquals(BlockType.SINGLE_EVENT, matchedEvent.getBlockType());
        assertNull(matchedEvent.getGroupBy()); // Null, as it's not set on rule
        assertNull(matchedEvent.getBlockParameters());
        assertEquals(1234, matchedEvent.getEventTime());
        assertEquals(matchingEvent, matchedEvent.getEventContent());
    }

    @Test
    void eventWithAggregationFields_ShouldBeSentToOutput() throws Exception {
        val ruleId = "ruleId";

        val rule = Rule.builder()
                .id(ruleId)
                .version(2)
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("processName", "process.exe"))
                        .aggregationGroupingFields(Arrays.asList("agg", "agg2"))
                        .build())).build();

        val map = Collections.singletonMap(ruleId, rule);
        ReadOnlyBroadcastState<String, Rule> ruleState = Mockito.mock(ReadOnlyBroadcastState.class);
        when(ruleState.immutableEntries()).thenReturn(map.entrySet());

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.ReadOnlyContext.class);
        Mockito.<ReadOnlyBroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);
        when(contextMock.timestamp()).thenReturn(1234L);

        val teg = TestEventGenerator.builder().build();
        val in = teg.generate("sourceHostName", "lavender", "processName", "process.exe", "agg", "one", "agg2", "two");

        val testFunction = new InputBroadcastProcessFunction();
        Collector<MatchedEvent> collector = Mockito.mock(Collector.class);
        testFunction.processElement(in, contextMock, collector);

        val outputCaptor = ArgumentCaptor.forClass(MatchedEvent.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());
        verify(collector, times(1)).collect(any(MatchedEvent.class));

        val matchedEvent = outputCaptor.getAllValues().get(0);
        assertEquals("agg=one,agg2=two", matchedEvent.getAggregationKey());
    }

    @Test
    void eventMatchingComplexRule_ShouldBeSentToOutput() throws Exception {
        val ruleId = "ruleId";
        val ruleCondition = new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2));
        val blockParams = Collections.singletonMap(BlockParameterKey.Threshold, "2");

        val rule = Rule.builder()
                .id(ruleId)
                .version(2)
                .blocks(Arrays.asList(
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "one")).build(),
                        Block.builder().type(BlockType.SINGLE_EVENT).condition(new EqualCondition("check", "two")).build(),
                        Block.builder()
                                .type(BlockType.THRESHOLD)
                                .condition(new EqualCondition("check", "three"))
                                .windowSize(2)
                                .windowSlide(1)
                                .parameters(blockParams)
                                .aggregationGroupingFields(Arrays.asList("sourceHostName", "processName"))
                                .build()
                ))
                .ruleCondition(ruleCondition)
                .groupByField("hostname")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val map = Collections.singletonMap(ruleId, rule);
        ReadOnlyBroadcastState<String, Rule> ruleState = Mockito.mock(ReadOnlyBroadcastState.class);
        when(ruleState.immutableEntries()).thenReturn(map.entrySet());

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.ReadOnlyContext.class);
        Mockito.<ReadOnlyBroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val teg = TestEventGenerator.builder().build();
        val matchingEvent = teg.generate("sourceHostName", "lavender", "processName", "process.exe", "check", "three", "hostname", "group");

        val testFunction = new InputBroadcastProcessFunction();
        testFunction.processElement(matchingEvent, contextMock, null);

        val outputCaptor = ArgumentCaptor.forClass(MatchedEvent.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());

        val matchedEvent = outputCaptor.getAllValues().get(0);
        assertEquals(ruleId, matchedEvent.getMatchedRuleId());
        assertEquals(2, matchedEvent.getMatchedRuleVersion());
        assertEquals(RuleType.COMPLEX, matchedEvent.getRuleType());
        assertEquals(ruleCondition, matchedEvent.getRuleCondition());
        assertEquals(10, matchedEvent.getRuleWindowSize());
        assertEquals(2, matchedEvent.getRuleWindowSlide());
        assertEquals(2, matchedEvent.getMatchedBlockIndex());
        assertEquals(BlockType.THRESHOLD, matchedEvent.getBlockType());
        assertEquals("hostname=group", matchedEvent.getGroupBy());
        assertEquals(blockParams, matchedEvent.getBlockParameters());
        assertEquals(2, matchedEvent.getWindowSize());
        assertEquals(1, matchedEvent.getWindowSlide());
        assertEquals(matchingEvent, matchedEvent.getEventContent());
        assertEquals("sourceHostName=lavender,processName=process.exe", matchedEvent.getAggregationKey());
    }

    @Test
    void eventWithoutGroupByField_ShouldNotBeSentToOutput() throws Exception {
        val ruleId = "ruleId";

        val rule = Rule.builder()
                .id(ruleId)
                .version(2)
                .blocks(Collections.singletonList(
                        Block.builder()
                                .type(BlockType.THRESHOLD)
                                .condition(new EqualCondition("check", "three"))
                                .windowSize(2)
                                .windowSlide(1)
                                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                                .build()
                ))
                .ruleCondition(new AllBlocksOccurredRuleCondition(Arrays.asList(0, 1, 2)))
                .groupByField("hostname")
                .windowSize(10)
                .windowSlide(2)
                .build();

        val map = Collections.singletonMap(ruleId, rule);
        ReadOnlyBroadcastState<String, Rule> ruleState = Mockito.mock(ReadOnlyBroadcastState.class);
        when(ruleState.immutableEntries()).thenReturn(map.entrySet());

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.ReadOnlyContext.class);
        Mockito.<ReadOnlyBroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val matchingEvent = "2019-02-19T09:50:24.065Z host CEF:0|Microsoft|Windows|1.0.0|21|Process Start|3|sourceHostName=lavender processName=process.exe check=three";

        val testFunction = new InputBroadcastProcessFunction();
        Collector<MatchedEvent> collector = Mockito.mock(Collector.class);
        testFunction.processElement(matchingEvent, contextMock, collector);

        verify(contextMock, never()).output(any(OutputTag.class), any());
        verify(collector, never()).collect(any());
    }

    @Test
    void inputNotMatchingCommand_ShouldNotBeSentToOutput() throws Exception {
        val ruleId = "ruleId";
        val rule = Rule.builder()
                .id(ruleId)
                .version(1)
                .blocks(Collections.singletonList(Block.builder()
                        .type(BlockType.SINGLE_EVENT)
                        .condition(new EqualCondition("processName", "evil.exe"))
                        .build())).build();

        val map = Collections.singletonMap(ruleId, rule);
        ReadOnlyBroadcastState<String, Rule> ruleState = Mockito.mock(ReadOnlyBroadcastState.class);
        when(ruleState.immutableEntries()).thenReturn(map.entrySet());

        val contextMock = Mockito.mock(InputBroadcastProcessFunction.ReadOnlyContext.class);
        Mockito.<ReadOnlyBroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val teg = TestEventGenerator.builder().build();
        val in = teg.generate("sourceHostName", "lavender", "processName", "process.exe");

        val testFunction = new InputBroadcastProcessFunction();
        Collector<MatchedEvent> collector = Mockito.mock(Collector.class);
        testFunction.processElement(in, contextMock, collector);

        verify(contextMock, never()).output(any(), any());
        verify(collector, never()).collect(any());
    }

    @Test
    void invalidCharInMessage_ShouldBeSentToOutput() {
        val ruleId = "ruleId";
        val ruleData = "{“blocks”:[{“type”:“SINGLE_EVENT”,“condition”:{“@class”:“uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition”,“key”:“groupBy”,“value”:“green”},“groupBy”:null,“windowSize”:0,“windowSlide”:0,“aggregationGroupingFields”:null,“parameters”:null}],“ruleCondition”:null,“windowSize”:0,“windowSlide”:0}";

        BroadcastState<String, Rule> ruleState = Mockito.mock(BroadcastState.class);
        val contextMock = Mockito.mock(InputBroadcastProcessFunction.Context.class);
        Mockito.<BroadcastState<String, Rule>>when(contextMock.getBroadcastState(any())).thenReturn(ruleState);

        val testFunction = new InputBroadcastProcessFunction();
        testFunction.processBroadcastElement(new ControlInput(ControlInputType.ADD_RULE, ruleId, 1, ruleData), contextMock, null);

        val outputCaptor = ArgumentCaptor.forClass(ControlOutput.class);
        verify(contextMock, times(1)).output(any(OutputTag.class), outputCaptor.capture());
        val capturedOutput = outputCaptor.getAllValues();
        assertEquals(ruleId, capturedOutput.get(0).getRuleId());
        assertEquals(ControlOutputStatus.ERROR, capturedOutput.get(0).getStatus());
    }
}
