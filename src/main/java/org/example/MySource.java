package org.example;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

class MySource implements SourceFunction<Row> {

    private volatile boolean isRunning = true;
    private final int tps;
    private final String mode;

    MySource(int tps, String mode) {
        this.tps = tps;
        this.mode = mode;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        while(isRunning) {
            Thread.sleep(1000);
            for (int i = 0; i < tps; ++i) {
                final long l = System.currentTimeMillis();
                if (mode.equals("string")) {
                    sourceContext.collect(Row.of("testKey" ,String.valueOf(l), i));
                } else if (mode.equals("hash")) {
                    Map<String, String> map = new HashMap<>();
                    map.put(String.valueOf(l), String.valueOf(i));
                    sourceContext.collect(Row.of("hashKey", map));
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}