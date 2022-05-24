package com.sample;

import com.sample.source.EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class PopAndBuyJoin {
    private static final Logger LOG = LoggerFactory.getLogger(PopAndBuyJoin.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.disableOperatorChaining(); // 关闭算子链
        env.setParallelism(64);

        WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy
                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.getFieldAs(2)));

        Configuration configuration = new Configuration();
        configuration.setLong("maxEventNum", 10000*10000);
        DataStream<Row> dataStreamSource = env.addSource(new EventSource(configuration)).name("Kafka");
        dataStreamSource = dataStreamSource
                .assignTimestampsAndWatermarks(watermarkStrategy);

        //定义OutputTag
        OutputTag<Row> outputTag1 = new OutputTag<Row>("pop") {};
        OutputTag<Row> outputTag2 = new OutputTag<Row>("buy") {};

        // 分流
        SingleOutputStreamOperator<Row> outputStream = dataStreamSource.process(new ProcessFunction<Row, Row>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row, Row>.Context ctx, Collector<Row> out) throws Exception {
                String label = value.getFieldAs(3);
                if (label.equals("0")) {
                    ctx.output(outputTag1, value);
                } else {
                    ctx.output(outputTag2, value);
                }
            }
        });

        DataStream<Row> popStream = outputStream.getSideOutput(outputTag1);
        DataStream<Row> buyStream = outputStream.getSideOutput(outputTag2);

        DataStream<Row> res =  popStream.join(buyStream)
                .where(row -> row.getField(3))
                .equalTo(row -> row.getField(3))
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<Row, Row, Row>() {
                    @Override
                    public Row join(Row first, Row second) throws Exception {
                        String user = first.getFieldAs(1);
                        long buyTime = Long.parseLong(second.getFieldAs(2));
                        long popTime = Long.parseLong(first.getFieldAs(2));
                        long timeDiff = buyTime - popTime;
                        return Row.of(user, timeDiff);
                    }
                });

        res.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                if (System.currentTimeMillis() % 10000 < 10) {
                    LOG.info(value.toString());
                }
            }
        });
        env.execute("GoThrough");
    }
}
