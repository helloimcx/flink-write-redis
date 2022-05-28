package com.sample;

import com.sample.source.EventSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
    private static final Logger LOG = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.disableOperatorChaining(); // 关闭算子链
        env.setParallelism(8);
        Configuration configuration = new Configuration();
        configuration.setLong("maxEventNum", 10000*10000);
        DataStream<Row> dataStreamSource = env.addSource(new EventSource(configuration)).name("Kafka");
        dataStreamSource.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                if (System.currentTimeMillis() % 10000 < 10) {
                    LOG.info(value.toString());
                }
            }
        }).name("RedisSink").setParallelism(4);
        env.execute("GoThrough");
    }
}
