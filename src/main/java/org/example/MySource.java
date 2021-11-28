package org.example;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

class MySource implements SourceFunction<String> {

    private volatile boolean isRunning = true;
    private final int tps;

    MySource(int tps) {
        this.tps = tps;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(isRunning) {
            Thread.sleep(1000);
            for (int i = 0; i < tps; ++i) {
                final long l = System.currentTimeMillis();
                sourceContext.collect(String.valueOf(l));
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}