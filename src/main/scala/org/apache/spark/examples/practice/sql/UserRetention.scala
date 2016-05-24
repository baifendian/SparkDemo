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
package org.apache.spark.examples.practice.sql

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import org.apache.log4j.Logger
import org.apache.spark.examples.practice.ml.TextCategoryV16._

/**
  * 计算留存率, 由于原始数据是 json 格式, 但是我们程序中保留在 hdfs 上的数据格式都是 parquet 格式.
  * 该程序应该在每天凌晨调度, 计算昨天的指标.
  *
  * 我们的原始数据是每天保存的, 如下所示(json):
  * origin-data-directory/{yyyy-mm-dd}/files
  *
  * 以日期 2016-05-24 为例, 我们会保留历史到 2016-05-24 的所有用户信息到 table {USER_IDS} 中, 如下所示(parquet):
  * {USER_IDS_DIR}/{yyyy-mm-dd}/files
  *
  * 以日期 2016-05-24 为例, 我们会保留当天新增用户的信息到 table {NEW_USER_IDS} 中, 如下所示(parquet):
  * {NEW_USER_IDS_DIR}/{yyyy-mm-dd}/files
  *
  * 以日期 2016-05-24 为例, 倒推一个时间, 比如 days, 我们计算 days 中的新增用户, 在 2016-05-24 这一天的留存, 保留到 mongodb 中, 格式为:
  * oriDate, retenDate, rentenRatio(*100%)
  *
  */
object UserRetention {
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // 获取当前要计算的日期
    val statDate: Date = args.length match {
      case x: Int if x > 1 => {
        val yesterday = args(0)
        if (!yesterday.matches("""\d{4}(\-)\d{2}(\-)\d{2}""")) {
          println(s"param format is not valid: ${args(0)}")
          System.exit(1)
        }

        val format = new SimpleDateFormat("yyyy-MM-dd")

        format.parse(yesterday)
      }
      case _ => {
        // 获取当前时间
        val now = new Date()
        val calendar = new GregorianCalendar()

        calendar.setTime(now)
        calendar.add(Calendar.DATE, -1)

        calendar.getTime
      }
    }

    logger.info(s"stat date is: ${statDate}")

    // 获取前 N 天的日期


    // 读取 hdfs 中的原始数据

    // 生成当前的 "所有用户" 放在 hive table 中

    // 生成当前的 "新增用户" 放在 hive table 中

    // 留存率放在 mongodb 中

  }
}