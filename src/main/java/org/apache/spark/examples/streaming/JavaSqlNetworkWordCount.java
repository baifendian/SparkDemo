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

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

/**
 * socket streaming 示例 （sql）
 * <p>
 * 
 * @author : dsfan
 * @date : 2016年4月1日
 */
public final class JavaSqlNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
            System.exit(1);
        }

        String hostname = args[0];
        Integer port = Integer.valueOf(args[1]);

        SparkConf sparkConf = new SparkConf().setAppName("JavaSqlNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // 创建一个socket stream
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostname, port, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x));
            }
        });

        // Convert RDDs of the words DStream to DataFrame and run SQL query
        words.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

            @Override
            public void call(JavaRDD<String> rdd, Time time) throws Exception {
                // 获取SQLContext（单例）
                SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());

                // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
                JavaRDD<JavaRecord> rowRDD = rdd.map(new Function<String, JavaRecord>() {
                    @Override
                    public JavaRecord call(String word) {
                        JavaRecord record = new JavaRecord();
                        record.setWord(word);
                        return record;
                    }
                });
                DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD, JavaRecord.class);

                // 注册为 table
                wordsDataFrame.registerTempTable("words");

                // 执行SQL查询count
                DataFrame wordCountsDataFrame = sqlContext.sql("select word, count(*) as total from words group by word");
                System.out.println("========= " + time + "=========");
                wordCountsDataFrame.show();
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}

/** 用于获取单例SQLContext */
class JavaSQLContextSingleton {
    static private transient SQLContext instance = null;

    static public SQLContext getInstance(SparkContext sparkContext) {
        if (instance == null) {
            instance = new SQLContext(sparkContext);
        }
        return instance;
    }
}

class JavaRecord implements java.io.Serializable {
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
