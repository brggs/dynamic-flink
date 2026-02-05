package uk.co.brggs.dynamicflink.blocks.uniquethreshold;

import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.BlockProcessor;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import uk.co.brggs.dynamicflink.blocks.MatchedEvent;
import uk.co.brggs.dynamicflink.events.InputEvent;
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
public class UniqueThresholdBlockProcessor implements BlockProcessor {
    private static final OutputTag<MatchedEvent> outputTag = new OutputTag<>(BlockType.UNIQUE_THRESHOLD.toString(), org.apache.flink.api.common.typeinfo.TypeInformation.of(MatchedEvent.class));

    @Override
    public DataStream<MatchedBlock> processEvents(SingleOutputStreamOperator<MatchedEvent> inputStream) {
        // Events matching Unique Value blocks are passed into a keyed window in order to be counted
        return inputStream.map(e -> {
            System.out.println("DEBUG: Processor received event: " + e);
            return e;
        }).filter(event -> {
            boolean keep = event.getBlockType() == BlockType.UNIQUE_THRESHOLD;
            if (keep) {
                System.out.println("DEBUG: Keeping event in processor: " + event.getEventContent());
            } else {
                 // System.out.println("DEBUG: Dropping event in processor: " + event.getBlockType());
            }
            return keep;
        })
                // Check that a value is present in the Unique Field, discard events where the field is not set
                .flatMap(new SafeFlatMapFunction<>((element, out) -> {
                    val uniqueField = element.getBlockParameters().get(BlockParameterKey.UniqueField);
                    val currentValue = new InputEvent(element.getEventContent()).getField(uniqueField);

                    if (currentValue != null && !currentValue.isEmpty()) {
                        System.out.println("DEBUG: Event passed filter. Unique value: " + currentValue);
                        out.collect(element);
                    }
                }, MatchedEvent.class))
                .name("unique-threshold-block-filter")
                .uid("unique-threshold-block-filter")
                .keyBy("customer", "matchedRuleId", "groupBy")
                .window(new SlidingWindowAssigner())
                .trigger(new UniqueThresholdWindowTrigger())
                .aggregate(new MatchedEventAggregateFunction(), new AggregatedEventProcessWindowFunction())
                .name("unique-threshold-block-aggregator")
                .uid("unique-threshold-block-aggregator")
                .flatMap(new SafeFlatMapFunction<>((block, out) -> {
                    block.setMatchMessage("Unique threshold exceeded.");
                    out.collect(block);
                }, MatchedBlock.class))
                .name("unique-threshold-block-mapper")
                .uid("unique-threshold-block-mapper");
    }
}
