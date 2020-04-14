package uk.co.brggs.dynamicflink.blocks.threshold;

import uk.co.brggs.dynamicflink.blocks.*;
import uk.co.brggs.dynamicflink.functions.SafeFlatMapFunction;
import uk.co.brggs.dynamicflink.windows.AggregatedEventProcessWindowFunction;
import uk.co.brggs.dynamicflink.windows.MatchedEventAggregateFunction;
import uk.co.brggs.dynamicflink.windows.SlidingWindowAssigner;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

@Slf4j
public class ThresholdBlockProcessor implements BlockProcessor {
    private static final OutputTag<MatchedEvent> outputTag = new OutputTag<MatchedEvent>(BlockType.THRESHOLD.toString()) {};

    @Override
    public DataStream<MatchedBlock> processEvents(SingleOutputStreamOperator<MatchedEvent> inputStream) {
        // Events matching Threshold blocks are passed into a keyed window in order to be counted
        return inputStream.getSideOutput(outputTag)
                .keyBy("customer", "matchedRuleId", "groupBy")
                .window(new SlidingWindowAssigner())
                .trigger(new ThresholdWindowTrigger())
                .aggregate(new MatchedEventAggregateFunction(), new AggregatedEventProcessWindowFunction())
                .name("threshold-block-aggregator")
                .uid("threshold-block-aggregator")
                .flatMap(new SafeFlatMapFunction<>((block, out) -> {
                    val totalCount = block.getMatchingEvents()
                        .stream()
                        .map(InternalEventSummary::getCount)
                        .reduce(0, Integer::sum);
                    val message = "Threshold exceeded. %d events were observed in the last %d minutes.";
                    block.setMatchMessage(String.format(message, totalCount, block.getWindowSize() / 60));
                    out.collect(block);
                }, MatchedBlock.class))
                .name("threshold-block-mapper")
                .uid("threshold-block-mapper");
    }
}



