package uk.co.brggs.dynamicflink.control;

import org.apache.flink.util.OutputTag;

public class ControlOutputTag {
    public static final OutputTag<ControlOutput> controlOutput = new OutputTag<ControlOutput>("control-output") {};
}
