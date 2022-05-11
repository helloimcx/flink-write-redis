package com.sample;

import com.sample.bean.Item;
import com.sample.source.EventSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class Test {
    private static final Logger LOG = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) throws Exception {
        // final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStreamSource = env.addSource(new EventSource());
        dataStreamSource.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                // LOG.info(value.toString());
            }
        });
        env.execute("Test");
    }
}
