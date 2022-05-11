package com.sample.source;

import com.sample.bean.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.util.LinkedList;
import java.util.Queue;

public class EventSource extends RichParallelSourceFunction<Row> {

    /** Flag to make the source cancelable. */
    private volatile boolean isRunning = true;

    /** The number of elements emitted already. */
    private long numElementsEmitted = 0;
    private transient Counter counter;

    final long maxEventNum = 10000 * 1000;
    Queue<Event> popQueue = new LinkedList<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("numElementsEmitted");
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        Event event;
        while (isRunning) {
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
            sourceContext.collect(Row.of(event.toString()));
            ++numElementsEmitted;
            this.counter.inc();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
