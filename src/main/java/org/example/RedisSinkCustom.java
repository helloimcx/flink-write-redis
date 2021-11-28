package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;

public class RedisSinkCustom extends RichSinkFunction<String> {
    Configuration parameters;
    Jedis jedis = null;
    JedisPool jedisPool = null;
    int max_tps;
    int interval;
    int parallelism;
    long startTime;
    long lastTimeWrite;
    long last10batchWriteTime = -1;
    int batch_size;
    long batch_count;
    ArrayList<String> arrayList;
    final Logger log = LoggerFactory.getLogger(RedisSinkCustom.class);


    public RedisSinkCustom(Configuration parameters) {
        this.parameters = parameters;
        this.max_tps = parameters.getInteger("max_tps", 1000);
        this.lastTimeWrite = -1;
        this.startTime = System.currentTimeMillis();
        this.batch_size = 1000;
        this.batch_count = 0;
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
    public void invoke(String value, Context context) throws InterruptedException {
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
                for (String s : arrayList) {
                    p.set("testKey", s);
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