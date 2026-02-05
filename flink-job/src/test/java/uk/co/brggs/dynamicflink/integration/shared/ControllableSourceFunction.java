package uk.co.brggs.dynamicflink.integration.shared;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public class ControllableSourceFunction<T> extends RichParallelSourceFunction<T> {

    private static final ConcurrentMap<String, CountDownLatch> startLatches = new ConcurrentHashMap<>();

    private final String name;
    private final List<T> itemsForOutput;

    private boolean running = true;

    ControllableSourceFunction(String name, List<T> itemsForOutput) {
        this.name = name;
        this.itemsForOutput = itemsForOutput;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        final int index = getRuntimeContext().getIndexOfThisSubtask();

        final CountDownLatch startLatch = startLatches.computeIfAbsent(getId(index), ignored -> new CountDownLatch(1));

        startLatch.await();
        int counter = 0;

        while (running && counter < itemsForOutput.size()) {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(itemsForOutput.get(counter++));
            }
        }

        // Emit a final watermark to close any open windows
        synchronized (sourceContext.getCheckpointLock()) {
            sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private String getId(int index) {
        return name + '_' + index;
    }

    static void startExecution(ControllableSourceFunction source, int index) {
        final CountDownLatch startLatch = startLatches.computeIfAbsent(source.getId(index), ignored -> new CountDownLatch(1));
        startLatch.countDown();
    }
}