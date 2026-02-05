package uk.co.brggs.dynamicflink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.OutputTag;
import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;
import uk.co.brggs.dynamicflink.control.*;
import uk.co.brggs.dynamicflink.events.InputEvent;
import uk.co.brggs.dynamicflink.rules.Rule;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import uk.co.brggs.dynamicflink.rules.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles and responds to control events, and matches input events against rules.
 */
@Slf4j
public class InputBroadcastProcessFunction
        extends BroadcastProcessFunction<String, ControlInput, MatchedEvent> {

    private final MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
            "RulesBroadcastState",
            Types.STRING,
            Types.POJO(Rule.class));

    private transient long maxProcessingTime = 0;

    private transient ObjectMapper objectMapper;

    /**
     * Performs actions when the function is initialised
     */
    @Override
    public void open(Configuration config) {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("maxProcessingTime", (Gauge<Long>) () -> maxProcessingTime);
    }

    /**
     * Processes control data
     *
     * @param controlInput Control instructions, used to add/remove rules.
     * @param ctx          Provides write access to the broadcast state.
     * @param collector    Not used. Acknowledgments are sent through side-output.
     */
    @Override
    public void processBroadcastElement(ControlInput controlInput, Context ctx, Collector<MatchedEvent> collector) {
        val ruleId = controlInput.getRuleId();
        val ruleVersion = controlInput.getRuleVersion();
        val content = controlInput.getContent();
        val controlType = controlInput.getType();

        ControlOutput output;

        val validationErrors = controlInput.validate();
        if (!validationErrors.isEmpty()) {
            val message = "Control Input failed validation: " + String.join(" ", validationErrors);
            output = new ControlOutput(controlInput, ControlOutputStatus.ERROR, message);
            log.warn(message);
        } else {
            switch (controlType) {
                case ADD_RULE: {
                    val rules = ctx.getBroadcastState(ruleStateDescriptor);
                    try {
                        if (rules.contains(ruleId) && rules.get(ruleId).getVersion() >= ruleVersion) {
                            // Do not apply a rule unless its version is higher than the current one
                            log.info("Rule {}, version {} already present. No action taken.", ruleId, rules.get(ruleId).getVersion());
                            output = new ControlOutput(controlInput, ControlOutputStatus.RULE_ACTIVE);
                            output.setRuleVersion(rules.get(ruleId).getVersion());
                        } else {
                            val newRule = new ObjectMapper().readValue(content, Rule.class);

                            // Store the ID and version number in the rule object
                            newRule.setId(ruleId);
                            newRule.setVersion(ruleVersion);
                            rules.put(ruleId, newRule);

                            output = new ControlOutput(controlInput, ControlOutputStatus.RULE_ACTIVE);
                            log.info("Rule added, rule {}, version {}", ruleId, ruleVersion);
                        }
                    } catch (Exception ex) {
                        output = new ControlOutput(controlInput, ControlOutputStatus.ERROR, ex.getMessage());
                        log.error("Error processing rule {}, content {}, version {}", controlInput.getRuleId(), content, ruleVersion, ex);
                    }
                    break;
                }
                case REMOVE_RULE: {
                    try {
                        val rules = ctx.getBroadcastState(ruleStateDescriptor);

                        if (!rules.contains(ruleId)) {
                            output = new ControlOutput(controlInput, ControlOutputStatus.RULE_DEACTIVATED);
                            log.info("Rule was not present, it may have already been removed.  Rule {}, version {}", ruleId, ruleVersion);
                        } else {
                            val ruleToBeRemoved = rules.get(ruleId);

                            if (ruleToBeRemoved.getVersion() != ruleVersion) {
                                throw new Exception(String.format(
                                        "Rule version %d is not the latest version of rule %s.  Latest version: %d",
                                        ruleVersion,
                                        ruleId,
                                        ruleToBeRemoved.getVersion()));
                            }

                            rules.remove(ruleId);
                            output = new ControlOutput(controlInput, ControlOutputStatus.RULE_DEACTIVATED);
                            log.info("Rule removed, rule {}, version {}", ruleId, ruleVersion);
                        }
                    } catch (Exception ex) {
                        output = new ControlOutput(controlInput, ControlOutputStatus.ERROR, ex.getMessage());
                        log.error("Error processing request to remove rule {}, content {}, version {}", ruleId, content, ruleVersion, ex);
                    }
                    break;
                }
                case QUERY_STATUS:
                    try {
                        val rules = ctx.getBroadcastState(ruleStateDescriptor);
                        val rulesForOutput = new ArrayList<RuleData>();
                        for (Map.Entry<String, Rule> e : rules.entries()) {
                            rulesForOutput.add(
                                    new RuleData(
                                            e.getKey(),
                                            e.getValue().getVersion(),
                                            getObjectMapper().writeValueAsString(e.getValue())));
                        }
                        val serialisedRules = getObjectMapper().writeValueAsString(rulesForOutput);
                        output = new ControlOutput(controlInput, ControlOutputStatus.STATUS_UPDATE, serialisedRules);
                    } catch (Exception ex) {
                        output = new ControlOutput(controlInput, ControlOutputStatus.ERROR, ex.getMessage());
                        log.error("Error processing request to query status.", ex);
                    }
                    break;
                default:
                    output = new ControlOutput(controlInput, ControlOutputStatus.ERROR, "Unknown controlType " + controlType);
                    break;
            }
        }

        try {
            ctx.output(ControlOutputTag.controlOutput, output);
        } catch (Exception ex) {
            log.error("Error sending output {}", output, ex);
        }
    }

    /**
     * Processes events
     *
     * @param eventContent The event data to be processed.
     * @param ctx          Provides read only access to the broadcast state (rules).
     * @param collector    Not used. Matches are sent through side-output.
     */
    @Override
    public void processElement(String eventContent, ReadOnlyContext ctx, Collector<MatchedEvent> collector) {
        try {
            val startTime = System.currentTimeMillis();

            val inputEvent = new InputEvent(eventContent);

            // gets all rules and check conditions in blocks
            // this eliminates all not related events from processing
            ctx.getBroadcastState(ruleStateDescriptor).immutableEntries().forEach(ruleEntry -> {
                val rule = ruleEntry.getValue();
                for (int i = 0; i < rule.getBlocks().size(); i++) {
                    try {
                        val block = rule.getBlocks().get(i);

                        if (block.getCondition().checkMatch(inputEvent)) {
                            val matchBuilder = populateMatchedEvent(rule, block, i, ctx.timestamp(), inputEvent);

                            val groupByField = rule.getGroupByField();
                            if (groupByField != null && !groupByField.isEmpty()) {
                                val groupByValue = inputEvent.getField(groupByField);
                                if (groupByValue == null || groupByValue.isEmpty()) {
                                    // If the group by field is not present in the event, it is not a match
                                    break;
                                }
                                matchBuilder.groupBy(String.format("%s=%s", groupByField, groupByValue));
                            }

                            log.debug("Rule matched: ruleId={}, blockType={}, event={}", rule.getId(), block.getType(), inputEvent.getContent());
                            val event = matchBuilder.build();
                            ctx.output(new OutputTag<>(block.getType().toString(), org.apache.flink.api.common.typeinfo.TypeInformation.of(MatchedEvent.class)), event);
                            collector.collect(event);
                            log.debug("Collected matched event for ruleId={}", rule.getId());
                        }

                    } catch (Throwable ex) {
                        log.error("Error processing rule {}, block {} for event {}", rule.getId(), i, eventContent, ex);
                        ex.printStackTrace();
                    }
                }
            });

            val duration = (System.currentTimeMillis() - startTime);
            if (duration > maxProcessingTime) {
                maxProcessingTime = duration;
            }
        } catch (Exception ex) {
            log.error("Error processing event {}", eventContent, ex);
        }
    }

    private MatchedEvent.MatchedEventBuilder populateMatchedEvent(
            Rule rule,
            Block block,
            int blockIndex,
            long timestamp,
            InputEvent event
    ) {
        val matchBuilder = MatchedEvent.builder()
                .customer(event.getField(InputEvent.CustomerField))
                .matchedRuleId(rule.getId())
                .matchedRuleVersion(rule.getVersion())
                .baseSeverity(rule.getBaseSeverity())
                .matchedBlockIndex(blockIndex)
                .blockType(block.getType())
                .windowSize(block.getWindowSize())
                .windowSlide(block.getWindowSlide())
                .ruleCondition(rule.getRuleCondition())
                .ruleWindowSize(rule.getWindowSize())
                .ruleWindowSlide(rule.getWindowSlide())
                .eventTime(timestamp)
                .eventContent(event.getContent());

        if (block.getAggregationGroupingFields() != null) {
            matchBuilder.aggregationKey(
                    block.getAggregationGroupingFields().stream()
                            .map(f -> String.format("%s=%s", f, event.getField(f)))
                            .collect(Collectors.joining(",")));
        }

        matchBuilder.ruleType(rule.getBlocks().size() > 1 ? RuleType.COMPLEX : RuleType.SIMPLE);

        if (block.getParameters() != null) {
            matchBuilder.blockParameters(block.getParameters());
        }

        return matchBuilder;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }
}
