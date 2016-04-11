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

package org.apache.spark.examples.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println( s"""
                             |Usage: <brokers> <topics>
                             |<brokers> is a list of one or more Kafka brokers
                             |<topics> is a list of one or more kafka topics to consume from
                             |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").
      set("spark.streaming.kafka.maxRatePerPartition", "1000")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    // Direct 方式是在 Spark 1.3 引入的，这种方式保证了数据能够正常处理，这种方式会周期性的查询 Kafka 关于 latest offsets(每个 topic + partition)中，
    // 当处理数据的 job 启动时，Kafka 的 simple API 会读取指定 ranges 中的数据，这种方式有几种优点:
    // 1. 简化并行化：对每个 kafka 中的 partition，有一个 RDD 相对应。
    // 2. 高效：避免数据丢失的同时不需要 Write Ahead Log.
    // 3. Exactly-once semantics.
    // 缺点是这种方式没有更新 zk，基于 zk 的监控工具无法有效监控
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    var offsetRanges = Array[OffsetRange]()

    val lines = messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)

    println("Word counts print...")
    wordCounts.print()

    println("Word counts offset...")
    wordCounts.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}