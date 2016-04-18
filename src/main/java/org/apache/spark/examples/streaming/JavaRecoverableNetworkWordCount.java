/**
 * Copyright (C) 2015 Baifendian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * 从socket stream 中计算 wordcount 将结果写入redis
 * <p>
 * 
 * @author : dsfan
 * @date : 2016年4月13日
 */
public final class JavaRecoverableNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("You arguments were " + Arrays.asList(args));
            System.err.println("Usage: JavaRecoverableNetworkWordCount <hostname> <port> <checkpoint-directory> <redisip> <redisport>");

            System.exit(1);
        }

        final String ip = args[0];
        final int port = Integer.parseInt(args[1]);
        final String checkpointDirectory = args[2];
        final String redisIp = args[3];
        final int redisPort = Integer.parseInt(args[4]);
        // 初始化JedisPoolHolder
        JedisPoolHolder.init(redisIp, redisPort);

        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
            @Override
            public JavaStreamingContext create() {
                return createContext(ip, port, checkpointDirectory);
            }
        };

        // 创建StreamingContext
        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
        ssc.start();
        ssc.awaitTermination();
    }

    private static JavaStreamingContext createContext(String ip, int port, String checkpointDirectory) {

        // 如果没有打印这一行，说明 StreamingContext 从 checkpoint 加载
        System.out.println("Creating new context");

        SparkConf sparkConf = new SparkConf().setAppName("JavaRecoverableNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        ssc.checkpoint(checkpointDirectory);

        // 创建 socket stream
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(ip, port);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x));
            }
        });
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        wordCounts.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rdd, Time time) throws IOException {
                List<Tuple2<String, Integer>> list = rdd.collect();
                Jedis jedis = null;
                try {
                    jedis = JedisPoolHolder.getInstance().getResource();
                    for (Tuple2<String, Integer> tuple2 : list) {
                        // 单词出现的次数记录到redis中（单词加前缀"word_"）
                        jedis.incrBy("word_" + tuple2._1(), tuple2._2());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (jedis != null) {
                        jedis.close();
                    }
                }
            }
        });

        return ssc;
    }
}

class JedisPoolHolder {
    /** {@link JedisPool} */
    private static volatile JedisPool instance = null;

    /** redis ip */
    private static String ip;

    /** redis port */
    private static int port;

    /**
     * 初始化
     * <p>
     *
     * @param redisIp
     * @param redisPort
     */
    public static void init(String redisIp, int redisPort) {
        ip = redisIp;
        port = redisPort;
    }

    /**
     * 获取单例JedisPool
     * <p>
     *
     * @return {@link JedisPool}
     */
    public static JedisPool getInstance() {
        if (instance == null) {
            synchronized (JedisPoolHolder.class) {
                if (instance == null) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(100);
                    config.setMinIdle(10);
                    config.setMaxIdle(10);
                    config.setMaxWaitMillis(2000);
                    config.setTestWhileIdle(false);
                    config.setTestOnBorrow(false);
                    config.setTestOnReturn(false);
                    instance = new JedisPool(config, ip, port);
                }
            }
        }
        return instance;
    }
}
