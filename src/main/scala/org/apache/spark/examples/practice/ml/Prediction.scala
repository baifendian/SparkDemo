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
package org.apache.spark.examples.practice.ml

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

object Prediction {
  private def functionToCreateContext(): StreamingContext = {
    // 加载配置文件, 配置文件示例为: conf.properties
    val sparkConf = new SparkConf().setAppName("TextCategory").
      set("spark.streaming.receiver.writeAheadLog.enable", "true"). // 先写日志, 提高容错性, 避免 receiver 挂掉
      set("spark.streaming.receiver.maxRate", "5000"). // 每秒的读取速率
      set("spark.streaming.stopGracefullyOnShutdown", "true"). // 设置为 true 会 gracefully 的关闭 StreamingContext
      set("spark.streaming.blockInterval", "1000ms") // block 的大小, 每个 block interval 的数据对应于一个 task

    // 创建 spark context 和 streaming context, 注意这里也设置了 checkpoint, 目的用于 stream 的状态恢复
    val ctx = new SparkContext(sparkConf)
    val ssc = new StreamingContext(ctx, Seconds(10))

    // check point 特性不是很稳定, 慎用!!!
    ssc.checkpoint("checkpoint/TextCategory")

    ssc
  }

  /**
    * 预测模型
    *
    */
  def predict: Unit = {
    // === 加载配置 ===
    val parser: ConfigParser = ConfigParser()

    // 初始化
    val ssc = StreamingContext.getOrCreate("checkpoint/TextCategory", functionToCreateContext _)
    val ctx = ssc.sparkContext
    val sqlContext = new SQLContext(ctx)

    // 自定义函数, 注意 array 类型为 Seq, 不能为 Array
    sqlContext.udf.register("strArrayMerge", (str: String, array: Seq[String]) => str +: array)

    val model = PipelineModel.load(parser.commonModelPath)

    // 读取流数据, 解析, 采用模型预测
    val kafkaStreams = (1 to parser.predictNumStreams).map { i => {
      val topicMap = parser.predictTopics.split(",").map((_, 1)).toMap
      KafkaUtils.createStream(ssc, parser.predictZookeeper, parser.predictGroupid, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    }
    }

    // message 进行 union, 注意我们并没有进行 repartition, 这是因为上面我们用了 "blockInterval"
    val messages = ssc.union(kafkaStreams)

    messages.foreachRDD { rdd =>
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word")

      // 使用模型进行预测
    }

    // 预测结果写入到 redis 中

    ctx.stop()
  }
}