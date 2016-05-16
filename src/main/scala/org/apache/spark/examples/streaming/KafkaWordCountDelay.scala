/**
  * Copyright (C) 2015 Baifendian Corporation
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.spark.examples.streaming

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 为了测试 spark 内部原理写的一个程序，读者可以忽略之
object KafkaWordCountDelay {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: <zkQuorum> <group> <topics> <numThreads> <dealyMillSeconds>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, dealyMillSeconds) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCountDelay") /*.
      set("spark.streaming.receiver.writeAheadLog.enable", "true").
      set("spark.streaming.kafka.maxRatePerPartition", "1000")*/

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // 设置 checkpoint，这是考虑到了有 window 操作，window 操作一般是需要进行 checkpoint
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // createStream 返回的是一个 Tuple2，具有 key，value，这里只关注 value.
    // 注意这里是 Receiver-based 方式（还提供了 non-receiver 模式），默认配置下，这种方式是会在 receiver 挂掉
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap /*, StorageLevel.MEMORY_AND_DISK_SER*/).map(_._2)
    val words = lines.flatMap(x => {
      Thread.sleep(dealyMillSeconds toInt)
      x.split(" ")
    })

    // 统计的是 10 分钟内的单词数量，每隔 10 秒统计 1 次
    val wordCounts = words.map(x => (x, 1L)).
      reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(2), 2).
      filter(x => x._2 > 0)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

// Produces some random words between 1 and 100.
object KafkaWordCountProducerBatch {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage> <durationSeconds>")
      System.exit(1)
    }

    // 需要注意的是这里是 broker list，为 host:port,host:port 形式
    val Array(brokers, topic, messagesPerSec, wordsPerMessage, durationSeconds) = args

    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    val startTime = System.currentTimeMillis()

    // 持续发送一段时间，是不是要爆了
    //while ((System.currentTimeMillis() - startTime) <= durationSeconds.toInt * 1000) {
    for (i <- 1 to durationSeconds.toInt) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }
}