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

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.log4j.Logger

// 获取 HDFS 连接
object HdfsConnection {
  val logger = Logger.getLogger(getClass.getName)

  // hdfs 的配置
  val conf: Configuration = new Configuration()
  // 文件系统句柄
  val fileSystem: FileSystem = FileSystem.get(conf)
  // format is "yyyy-mm-dd"
  var currentDay: String = null
  // the directory of hdfs path
  var currentPath: String = null

  // hdfs 的写入句柄, 注意多线程问题, 第一个参数是写入句柄, 第二个参数是当前的小时情况
  val writeHandler: ThreadLocal[(FSDataOutputStream, String)] = new ThreadLocal[(FSDataOutputStream, String)] {
    override def initialValue(): (FSDataOutputStream, String) =
      (null, null)
  }

  // 删除 checkpoint(这是由于 checkpoint 的不稳定特性导致)
  def removeCheckpoint(checkpoint: String): Unit = {
    fileSystem.delete(new Path(checkpoint), true)
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

      // 如果 "天" 已经过时, 那么会创建一个目录
      if (currentDay == null || currentDay != nowDay) {
        currentDay = nowDay

        // 创建新的目录
        val path = new Path(s"${currentPath}${File.separator}${currentDay}")

        logger.info(s"create dir: $path")

        if (!fileSystem.exists(path)) {
          fileSystem.mkdirs(path)
        }
      }

      // 获取句柄, 以及当前存储的时间
      val handler = writeHandler.get()._1
      val hour = writeHandler.get()._2

      // 如果 "小时" 已经过时, 也创建一个文件
      if (hour == null || hour != nowHour) {
        if (handler != null) {
          handler.close()
        }

        val newPath = new Path(s"${currentPath}${File.separator}${currentDay}${File.separator}${java.util.UUID.randomUUID.toString}-${nowHour}")

        logger.info(s"create file: $newPath")

        val fout: FSDataOutputStream = fileSystem.create(newPath)

        writeHandler.set((fout, nowHour))
      }

      // 返回最新的句柄
      val newHandler = writeHandler.get()._1

      newHandler
    }
  }
}