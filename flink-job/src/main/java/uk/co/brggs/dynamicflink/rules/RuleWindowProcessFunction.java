package uk.co.brggs.dynamicflink.rules;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import uk.co.brggs.dynamicflink.outputevents.OutputEvent;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

@Slf4j
public class RuleWindowProcessFunction extends ProcessWindowFunction<MatchedBlock, OutputEvent, Tuple, TimeWindow> {
    @Override
    public void process(Tuple ruleId, Context context, Iterable<MatchedBlock> elements, Collector<OutputEvent> out) {
        try {
            val matchedBlocks = new ArrayList<MatchedBlock>();
            elements.forEach(matchedBlocks::add);

            val firstBlock = matchedBlocks.get(0);
            val ruleCondition = firstBlock.getRuleCondition();

            if (ruleCondition.checkMatch(matchedBlocks)) {
                out.collect(OutputEvent.createFromMatchedBlocks(matchedBlocks));
            }
        } catch (Exception ex) {
            log.error("Error processing window for rule {}", ruleId, ex);
        }
    }
}
