package uk.co.brggs.dynamicflink.integration.shared;

import uk.co.brggs.dynamicflink.DynamicFlink;
import uk.co.brggs.dynamicflink.blocks.BlockProcessor;
import uk.co.brggs.dynamicflink.blocks.droptozero.DropToZeroBlockProcessor;
import uk.co.brggs.dynamicflink.blocks.simpleanomaly.SimpleAnomalyBlockProcessor;
import uk.co.brggs.dynamicflink.blocks.singleevent.SingleEventBlockProcessor;
import uk.co.brggs.dynamicflink.blocks.threshold.ThresholdBlockProcessor;
import uk.co.brggs.dynamicflink.blocks.uniquethreshold.UniqueThresholdBlockProcessor;
import uk.co.brggs.dynamicflink.control.ControlInput;
import uk.co.brggs.dynamicflink.control.ControlOutput;
import uk.co.brggs.dynamicflink.control.ControlOutputStatus;
import lombok.val;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import uk.co.brggs.dynamicflink.outputevents.OutputEvent;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class IntegrationTestCluster {
    private static final int NUM_TMS = 1;
    private static final int NUM_SLOTS = 1;
    private static final int PARALLELISM = NUM_SLOTS * NUM_TMS;

    private static final Configuration configuration = new Configuration();

    static {
        // Allow override via system property; default to 0 (ephemeral) for tests
        final String blobPort = System.getProperty("flink.blob.port", "0");
        configuration.setString(BlobServerOptions.PORT, blobPort);
    }

    private static final MiniClusterWithClientResource miniClusterWithClientResource = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(NUM_SLOTS)
                    .setNumberTaskManagers(NUM_TMS)
                    .setConfiguration(configuration)
                    .build());

    // Keeps track of the total control messages, so we know when they have all been received
    private static int controlCount;

    IntegrationTestCluster() {
        try {
            miniClusterWithClientResource.before();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate test cluster.", e);
        }
    }

    public void run(List<ControlInput> controlInput, List<String> events) throws Exception {
        // Capture log output
        val logWriter = new StringWriter();
        val writeAppender = addLogAppender(logWriter);

        // Clear the output from previous runs
        EventSink.values.clear();
        ControlSink.values.clear();
        ControlSink.allControlResponsesReceived = new CompletableFuture<>();

        controlCount = controlInput.size();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);


        // Generate random names, so that in subsequent tests runs the sources don't fire immediately
        val controlSource = new ControllableSourceFunction<ControlInput>(UUID.randomUUID().toString(), controlInput);
        val eventSource = new ControllableSourceFunction<String>(UUID.randomUUID().toString(), events);

        val controlStream = env.addSource(controlSource).name("Control Source").returns(ControlInput.class);
        val eventStream = env.addSource(eventSource).name("Event Source").returns(String.class);

        List<BlockProcessor> blocks = Arrays.asList(
                new SingleEventBlockProcessor(),
                new ThresholdBlockProcessor(),
                new UniqueThresholdBlockProcessor(),
                new SimpleAnomalyBlockProcessor(),
                new DropToZeroBlockProcessor()
        );

        DynamicFlink.build(eventStream, controlStream, new EventSink(), new ControlSink(), blocks);

        val jobGraph = env.getStreamGraph().getJobGraph();

        miniClusterWithClientResource.getMiniCluster().submitJob(jobGraph).get();

        val jobResultFuture = miniClusterWithClientResource.getMiniCluster().requestJobResult(jobGraph.getJobID());

        for (int i = 0; i < PARALLELISM; i++) {
            ControllableSourceFunction.startExecution(controlSource, i);
        }

        ControlSink.allControlResponsesReceived.join();

        // The system has acknowledged all control input, so now we can start sending events
        for (int i = 0; i < PARALLELISM; i++) {
            ControllableSourceFunction.startExecution(eventSource, i);
        }

        jobResultFuture.join();

        val controlErrors = ControlSink.values.stream()
                .filter(co -> co.getStatus() == ControlOutputStatus.ERROR)
                .map(ControlOutput::getContent)
                .collect(Collectors.toList());

        if (controlErrors.size() > 0) {
            throw new AssertionError(
                    "Control message was not processed correctly: " + String.join(",", controlErrors));
        }

        val logOutput = logWriter.toString();

        if (logOutput.contains("cannot be used as a POJO")) {
            throw new AssertionError("Invalid POJO detected, see log output.");
        }
        if (logOutput.contains("switched from state FAILING to FAILED")) {
            throw new AssertionError("Flink job failed, see log output.");
        }

        removeLogAppender(writeAppender);
    }

    private WriterAppender addLogAppender(StringWriter logWriter) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();

        PatternLayout layout = PatternLayout.newBuilder()
                .withPattern("%m%n")
                .withConfiguration(config)
                .build();

        WriterAppender wa = WriterAppender.newBuilder()
                .setName("integration-test-writer")
                .setTarget(logWriter)
                .setLayout(layout)
                .setConfiguration(config)
                .build();
        wa.start();

        org.apache.logging.log4j.core.Logger rootLogger = ctx.getRootLogger();
        rootLogger.addAppender(wa);

        return wa;
    }

    private void removeLogAppender(WriterAppender wa) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.Logger rootLogger = ctx.getRootLogger();
        rootLogger.removeAppender(wa);
        wa.stop();
    }

    public static class EventSink implements SinkFunction<OutputEvent> {
        // must be static
        public static final List<OutputEvent> values = new ArrayList<>();

        @Override
        public synchronized void invoke(OutputEvent value, Context ctx) {
            values.add(value);
        }
    }

    public static class ControlSink implements SinkFunction<ControlOutput> {
        // must be static
        public static final List<ControlOutput> values = new ArrayList<>();

        static CompletableFuture<Void> allControlResponsesReceived = new CompletableFuture<>();

        @Override
        public synchronized void invoke(ControlOutput value, Context ctx) {
            values.add(value);
            if (values.size() == controlCount) {
                allControlResponsesReceived.complete(null);
            }
        }
    }
}
