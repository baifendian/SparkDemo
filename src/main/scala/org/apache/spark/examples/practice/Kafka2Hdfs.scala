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
package org.apache.spark.examples.practice

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

// 参数的 key 值
object Params {
  val BROKERS = "brokers"
  val TOPICS = "topics"

  val REDIS_IP = "redis.ip"
  val REDIS_PORT = "redis.port"
  val REDIS_DB = "redis.db"

  val HDFS_PATH = "hdfs.path"
}

// 获取 HDFS 连接
object HdfsConnection {
  // hdfs 的配置
  val conf: Configuration = new Configuration()

  // 文件系统句柄
  val fileSystem: FileSystem = FileSystem.get(conf)

  // format is "yyyy-mm-dd"
  var currentDay: String = null

  // format is "yyyy-MM-dd-HH"
  var currentHour: String = null

  // the directory of hdfs path
  var currentPath: String = null

  // hdfs 的写入句柄, 注意多线程问题
  val writeHandler: ThreadLocal[(FSDataOutputStream, Long)] = new ThreadLocal[(FSDataOutputStream, Long)] {
    override def initialValue(): (FSDataOutputStream, Long) =
      (null, 0)
  }

  // 获取 hdfs 的连接
  def getHdfsConnection(props: Properties): FSDataOutputStream = {
    this.synchronized {
      // 如果第一次, 需要初始化
      if (currentPath == null) {
        currentPath = props.getProperty(Params.HDFS_PATH)
      }

      // 获取当前时间
      val now = new Date()

      // 如果当前时间不一致, 则会重新构建 Path
      val format1 = new SimpleDateFormat("yyyy-MM-dd")
      val format2 = new SimpleDateFormat("yyyy-MM-dd-HH")

      val nowDay = format1.format(now)
      val nowHour = format2.format(now)

      // 如果 "天" 已经过时
      if (currentDay == null || currentDay != nowDay) {
        currentDay = nowDay

        val path = new Path(s"${currentPath}${File.separator}${currentDay}")

        println(s"create dir: $path")

        if (!fileSystem.exists(path)) {
          fileSystem.mkdirs(path);
        }
      }

      // 如果 "小时" 已经过时
      if (currentHour == null || currentHour != nowHour) {
        currentHour = nowHour

        val handler = writeHandler.get()._1

        if (handler != null) {
          handler.close()
        }

        val path = new Path(s"${currentPath}${File.separator}${currentDay}${File.separator}${java.util.UUID.randomUUID.toString}-${currentHour}")

        println(s"create file: $path")

        val fout: FSDataOutputStream = fileSystem.create(path)

        writeHandler.set((fout, 0))
      }

      // 返回句柄
      var t = writeHandler.get()

      val handler = t._1

      // 如果超过 30 秒,则会进行强制 flush
      if(System.currentTimeMillis() - t._2 >= 30000) {
        handler.hflush()
        handler.hsync()

        writeHandler.set((handler, System.currentTimeMillis()))
      }

      handler
    }
  }
}

object Kafka2Hdfs {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: configuration")
      System.exit(1)
    }

    /* 加载配置文件, 配置文件示例为:
     *
     * brokers=ip1:port1,ip2:port2
     * topics=topic1,topic2,...
     *
     * redis.ip=host
     * redis.port=port
     * redis.db=db
     *
     * hdfs.path=xxx
     */
    val props = new Properties()
    props.load(new FileInputStream(args(0)))

    // 设置 spark Config
    val sparkConf = new SparkConf().setAppName("Kafka2Hdfs").
      set("spark.streaming.kafka.maxRatePerPartition", "1000")

    // 创建 spark context 和 streaming context
    val ctx = new SparkContext(sparkConf)
    val ssc = new StreamingContext(ctx, Seconds(5))

    val broadcastVar = ctx.broadcast(props)

    // 创建 kafka stream
    val topics = props.getProperty(Params.TOPICS)
    val brokers = props.getProperty(Params.BROKERS)

    println(s"topics: $topics, brokers: $brokers")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val hdfsPath = props.getProperty(Params.HDFS_PATH)

    println(s"hdfs path: $hdfsPath")

    // 对我们获取的数据, 进行处理, 保存到 hdfs 中
    messages.map(x => x._2).foreachRDD { rdd =>
      rdd.foreachPartition {
        // executed at the worker
        partitionOfRecords =>
          val connection = HdfsConnection.getHdfsConnection(broadcastVar.value)
          partitionOfRecords.foreach(
            record => {
              connection.writeUTF(record)
              connection.writeBytes("\n")
            }
          )
      }
    }

    // redis connection params
    val redisIp = props.getProperty(Params.REDIS_IP)
    val redisPort = props.getProperty(Params.REDIS_PORT)
    val redisDb = props.getProperty(Params.REDIS_DB)

    println(s"redis ip: $redisIp, redis port: $redisPort, redis db: $redisDb")

    // 对时间窗口的数据进行统计, 采用 sql 的分析方法

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}