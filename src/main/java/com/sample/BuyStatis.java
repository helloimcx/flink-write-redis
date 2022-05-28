package com.sample;

import com.sample.source.EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class BuyStatis {
    private static final Logger LOG = LoggerFactory.getLogger(BuyStatis.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // env.disableOperatorChaining(); // 关闭算子链
        env.setParallelism(8);

        WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy
                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.getFieldAs(2)));

        Configuration configuration = new Configuration();
        configuration.setLong("maxEventNum", 10000*10000);
        DataStream<Row> dataStreamSource = env.addSource(new EventSource(configuration)).name("Kafka").setParallelism(4);
        dataStreamSource = dataStreamSource
                .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<Row> dataStream = dataStreamSource.map((MapFunction<Row, Row>) value -> {
            String timestamp = value.getFieldAs(2);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String sd = sdf.format(new Date(Long.parseLong(timestamp)));
            value.setField(2, Timestamp.valueOf(sd).toLocalDateTime());
            value.setField(3, Integer.valueOf(value.getFieldAs(3)));
            return value;
        }).returns(
                Types.ROW_NAMED(
                        new String[] {"items", "user", "timestamp", "label", "type"},
                        Types.STRING, Types.STRING, Types.LOCAL_DATE_TIME, Types.INT, Types.STRING))
                .setParallelism(6);

        tableEnv.createTemporaryView(
                "InputTable",
                dataStream,
                Schema.newBuilder()
                        .column("items", DataTypes.STRING())
                        .column("user", DataTypes.STRING())
                        .column("timestamp", DataTypes.TIMESTAMP())
                        .column("label", DataTypes.INT())
                        .column("type", DataTypes.STRING())
                        .build());

        Table resultTable = tableEnv.sqlQuery(
                "SELECT user, SUM(label) FROM InputTable GROUP BY user");
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        resultStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                LOG.info(value.toString());

            }
        }).setParallelism(6).name("ConsoleSink");

        env.execute("BuyStatis");
    }
}
