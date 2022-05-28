package com.sample.source;

import com.sample.bean.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.util.LinkedList;
import java.util.Queue;

public class EventSource extends RichParallelSourceFunction<Row> implements CheckpointedFunction {

    /** Flag to make the source cancelable. */
    private volatile boolean isRunning = true;

    /** The number of elements emitted already. */
    private long numElementsEmitted = 0;
    // private transient Counter counter;

    long maxEventNum;
    Queue<Event> popQueue = new LinkedList<>();

    public EventSource(Configuration configuration) {
        maxEventNum = configuration.getLong("maxEventNum", 10);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        this.counter = getRuntimeContext()
//                .getMetricGroup()
//                .counter("numElementsEmitted");
        int parallelism = this.getRuntimeContext().getNumberOfParallelSubtasks();
        maxEventNum /= parallelism;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        final Object lock = sourceContext.getCheckpointLock();
        Event event;
        while (isRunning && numElementsEmitted < maxEventNum) {
            int label = 0;
            if (System.currentTimeMillis() % 100000 < 10) {
                label = 1;
            }
            if (label == 0 || popQueue.isEmpty()) {
                event = new Event(0);
                if (popQueue.size() < 10) {
                    popQueue.add(event);
                }
            } else {
                event = new Event(1, popQueue.poll());
            }
            synchronized (lock) {
                String[] arr = event.toString().split("\\|");
                Row row = new Row(arr.length);
                for (int i = 0; i < arr.length; ++i) {
                    row.setField(i, arr[i]);
                }
                sourceContext.collect(row);
                ++numElementsEmitted;
                // this.counter.inc();
            }
            Thread.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
