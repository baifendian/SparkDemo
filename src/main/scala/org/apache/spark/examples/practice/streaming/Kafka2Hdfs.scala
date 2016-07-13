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
package org.apache.spark.examples.practice.streaming

import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

// 配置文件的广播变量, 这里使用了单件模式, 为了避免 driver 挂掉
object BroadConfig {
  @volatile private var instance: Broadcast[Properties] = null

  def getInstance(sc: SparkContext, filename: String = "props"): Broadcast[Properties] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val props = new Properties()
          props.load(new FileInputStream(filename))

          instance = sc.broadcast(props)
        }
      }
    }

    instance
  }
}

object Kafka2Hdfs {
  val logger = Logger.getLogger(getClass.getName)

  private def functionToCreateContext(): StreamingContext = {
    // 加载配置文件, 配置文件示例为: conf.properties
    val sparkConf = new SparkConf().setAppName("Kafka2Hdfs").
      set("spark.streaming.receiver.writeAheadLog.enable", "true"). // 先写日志, 提高容错性, 避免 receiver 挂掉
      set("spark.streaming.receiver.maxRate", "5000"). // 每秒的读取速率
      set("spark.streaming.stopGracefullyOnShutdown", "true"). // 设置为 true 会 gracefully 的关闭 StreamingContext
      set("spark.streaming.blockInterval", "1000ms") // block 的大小, 每个 block interval 的数据对应于一个 task

    // 创建 spark context 和 streaming context, 注意这里也设置了 checkpoint, 目的用于 stream 的状态恢复
    val ctx = new SparkContext(sparkConf)
    val ssc = new StreamingContext(ctx, Seconds(10))

    // 设置 checkpoint
    ssc.checkpoint("checkpoint/Kafka2Hdfs")

    // 创建 kafka stream, 注意这段代码需要放在里面
    val topics = BroadConfig.getInstance(ctx).value.getProperty(Params.TOPICS)
    val zk = BroadConfig.getInstance(ctx).value.getProperty(Params.ZK)
    val group = BroadConfig.getInstance(ctx).value.getProperty(Params.GROUP)
    val numStreams = BroadConfig.getInstance(ctx).value.getProperty(Params.NUM_STREAMS) toInt

    logger.info(s"topics: $topics, zookeeper: $zk, group id: $group, num streams: $numStreams")

    // 注意这里是 receiver 方式, 我们会创建多个 streams, 这样多个 executor 都会执行 receiver(input DStream)
    val kafkaStreams = (1 to numStreams).map { i => {
      val topicMap = topics.split(",").map((_, 1)).toMap
      KafkaUtils.createStream(ssc, zk, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    }
    }

    // message 进行 union, 注意我们并没有进行 repartition, 这是因为上面我们用了 "blockInterval"
    val messages = ssc.union(kafkaStreams)

    val hdfsPath = BroadConfig.getInstance(ctx).value.getProperty(Params.HDFS_PATH)

    logger.info(s"hdfs path: $hdfsPath")

    // 对我们获取的数据, 进行处理, 保存到 hdfs 中
    messages.map(x => x._2).foreachRDD { rdd =>
      // only can be execution on driver
      val config = BroadConfig.getInstance(rdd.sparkContext).value
      // executed at the worker
      rdd.foreachPartition {
        partitionOfRecords =>
          val connection = HdfsConnection.getHdfsConnection(config)
          partitionOfRecords.foreach(
            record => {
              // connection.writeUTF(record)
              connection.write(record.getBytes("UTF-8"))
              connection.writeBytes("\n")
            }
          )
          // 每次完了之后进行 flush
          try {
            connection.hflush()
          } catch {
            case e: Exception => logger.error(s"hflush exception: ${e.getMessage}")
          }
      }
    }

    ssc
  }

  def main(args: Array[String]) {
    // 注意我们这里有个 checkpoint 的恢复机制, 应对 driver 的重启(从 metadata 恢复), 另外也可以应对有状态的操作(不过本示例没有)
    val ssc = StreamingContext.getOrCreate("checkpoint/Kafka2Hdfs", functionToCreateContext _)

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}