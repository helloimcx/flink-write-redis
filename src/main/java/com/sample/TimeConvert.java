package com.sample;

import com.sample.bean.Item;
import com.sample.source.EventSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class TimeConvert {
    private static final Logger LOG = LoggerFactory.getLogger(TimeConvert.class);

    public static String timeStamp2Date(String seconds) {
        if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(Long.parseLong(seconds + "000")));
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.disableOperatorChaining(); // 关闭算子链
        env.setParallelism(64);
        Configuration configuration = new Configuration();
        configuration.setLong("maxEventNum", 10000*10000);
        DataStream<Row> dataStreamSource = env.addSource(new EventSource(configuration)).name("Kafka");

        dataStreamSource = dataStreamSource.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                String[] arr = value.toString().split("\\|");
                arr[2] = timeStamp2Date(arr[2]);
                Row row = new Row(arr.length);
                for (int i = 0; i < arr.length; ++i) {
                    row.setField(i, arr[i]);
                }
                return row;
            }
        });

        dataStreamSource.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                if (System.currentTimeMillis() % 10000 < 10) {
                    LOG.info(value.toString());
                }
            }
        });
        env.execute("TimeConvert");
    }
}
