package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.Map;

public class RedisSinkCustom extends RichSinkFunction<Row> {
    Configuration parameters; // 配置参数
    Jedis jedis = null;
    JedisPool jedisPool = null;
    int max_tps; // 最大tps
    int interval; // 每次写入的时间间隔
    int parallelism; // 写入并行度
    int batch_size = 1000; // 每次写入的数据量
    long lastTimeWrite = -1; // 上次写入的时间
    long last10batchWriteTime = -1; // 上10个batch写入的时间
    long batch_count = 0; // 写入的batch数量
    ArrayList<Row> arrayList; // 缓存待写入的数据
    String separator; // 分隔符
    String mode; // 写入redis的数据结构
    int expire; // 数据过期时间
    final Logger log = LoggerFactory.getLogger(RedisSinkCustom.class);


    public RedisSinkCustom(Configuration parameters) {
        this.parameters = parameters;
        this.max_tps = parameters.getInteger("max_tps", 1000);
        this.separator = parameters.getString("separator", ";");
        this.mode = parameters.getString("mode", "string");
        this.expire = parameters.getInteger("expire", 10);
        this.arrayList = new ArrayList<>();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(this.parameters);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(jedisPoolConfig,
                this.parameters.getString("host", "hadoop"),
                this.parameters.getInteger("port", 6379),
                10000,
                this.parameters.getString("password", ""));

        this.parallelism = this.getRuntimeContext().getNumberOfParallelSubtasks();
        int writePS = max_tps / batch_size / this.parallelism;
        writePS = Math.max(1, writePS);
        this.interval = 1000 / writePS;
        log.info("sink并性度: " + this.parallelism);
    }

    @Override
    public void invoke(Row value, Context context) throws InterruptedException {
        arrayList.add(value);
        if (arrayList.size() == batch_size) {
            if (interval > 0) {
                Thread.sleep(interval);
            }
//            long curTimeMills = System.currentTimeMillis();
//            long gap = curTimeMills - lastTimeWrite;
//            if (gap > 0 && gap < interval) {
//                Thread.sleep(interval-gap);
//                log.info("Thread.sleep(interval-gap): " + (interval-gap));
//            }
            try {
                jedis = jedisPool.getResource();
                Pipeline p = jedis.pipelined();
                for (Row row : arrayList) {
                    String k =  String.valueOf(row.getField(0));
                    if (mode.equals("string")) { // string方式写入
                        StringBuilder v = new StringBuilder(k);
                        for (int i = 1; i < row.getArity(); ++i) {
                            v.append(separator).append(row.getField(i));
                        }
                        p.set(k, v.toString());
                        p.expire(k, expire);
                    } else if (mode.equals("hash")) { // hash方式写入
                        if (row.getArity() != 2) {
                            throw new RuntimeException("数据格式错误");
                        }
                        Map<String, String> map = (Map<String, String>) row.getField(1);
                        if (map != null) {
                            for (Map.Entry<String, String> entry : map.entrySet()) {
                                p.hset(k, entry.getKey(), entry.getValue());
                            }
                        }
                        p.expire(k, expire);
                    }
                }
                p.sync();
            } finally {
                arrayList.clear();
                lastTimeWrite = System.currentTimeMillis();
                if (jedis != null) {
                    jedis.close();
                }
            }

            // 每10个batch统计一次速度
            batch_count = batch_count % 10 + 1; // 统计最近10个batch的速度
            if (batch_count == 10) {
                if (last10batchWriteTime != -1) {
                    long gap = lastTimeWrite - last10batchWriteTime;
                    double speed = parallelism * batch_size * batch_count / ((double)gap / 1000);
                    if (speed > max_tps + 10) {
                        interval += 10;
                        // log.info("speed too fast: " + speed + " interval: " + interval) ;
                    } else if (speed < max_tps - 10 && interval > 0) {
                        interval -= 10;
                        // log.info("speed too slow:" + speed + " interval: " + interval);
                    }
                }
                last10batchWriteTime = lastTimeWrite;
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}