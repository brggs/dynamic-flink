package uk.co.brggs.dynamicflink.rules;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Splits a stream to side outputs, using the rule type as the tag.
 */
public class RuleTypeStreamSplitter extends ProcessFunction<MatchedBlock, MatchedBlock> {

    @Override
    public void processElement(
            MatchedBlock value,
            Context ctx,
            Collector<MatchedBlock> out) {

        // Emit data to the appropriate side output
        ctx.output(new OutputTag<MatchedBlock>(value.getRuleType().toString()) {}, value);
    }
}
