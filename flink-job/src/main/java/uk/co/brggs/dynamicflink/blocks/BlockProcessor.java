package uk.co.brggs.dynamicflink.blocks;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface BlockProcessor {
    DataStream<MatchedBlock> processEvents(SingleOutputStreamOperator<MatchedEvent> inputStream);
}
