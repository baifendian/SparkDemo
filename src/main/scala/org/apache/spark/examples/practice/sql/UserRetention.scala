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

import com.mongodb.casbah.commons.MongoDBObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

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

  private val APPKEY = "appkey"
  private val GID = "gid"
  private val DATE = "date"

  private val BASE_DAY = "baseDay"
  private val STAT_DAY = "statDay"

  /**
    *
    * @param args
    * @return
    */
  private def getDays(args: Array[String]): (String, Array[String]) = {
    // 获取当前要计算的日期
    val format = new SimpleDateFormat("yyyy-MM-dd")

    // 获取统计的日期
    val statDate: Date = args.length match {
      case x: Int if x > 0 => {
        val yesterday = args(0)
        if (!yesterday.matches("""\d{4}(\-)\d{2}(\-)\d{2}""")) {
          logger.error(s"param format is not valid: ${args(0)}")
          System.exit(1)
        }

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

    // 获取前 N 天的日期
    val days =
      for (i <- 1 to (Params.days)) yield {
        val calendar = new GregorianCalendar()
        calendar.setTime(statDate)
        calendar.add(Calendar.DATE, -1 * i)
        new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
      }

    (format.format(statDate), days toArray)
  }

  /**
    * 建表, 设置环境变量
    *
    * @param hiveSqlContext
    */
  private def init(statDay: String, hiveSqlContext: HiveContext): Unit = {
    logger.info(s"create table: ${Params.hiveAlluserCreateTable}")
    hiveSqlContext.sql(Params.hiveAlluserCreateTable)

    logger.info(s"create table: ${Params.hiveNewuserCreateTable}")
    hiveSqlContext.sql(Params.hiveNewuserCreateTable)

    logger.info(s"delete table data in ${statDay}")
    if (Params.hiveStatdayDelete) {
      val conf: Configuration = new Configuration()
      val fileSystem: FileSystem = FileSystem.get(conf)

      fileSystem.delete(new Path(s"${Params.hiveWarehouseDir}/${Params.hiveAlluserCreateTable}/${DATE}=${statDay}"), true)
      fileSystem.delete(new Path(s"${Params.hiveWarehouseDir}/${Params.hiveNewuserCreateTable}/${DATE}=${statDay}"), true)
    }

    // 需要动态 partition
    hiveSqlContext.setConf("hive.exec.dynamic.partition", "true")
    hiveSqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // 做一个索引
    MongoDBConnection.getInstance.collConn.createIndex(MongoDBObject(APPKEY -> true, BASE_DAY -> true, STAT_DAY -> true))
  }

  /**
    * 显示一些调试信息
    *
    * @param data
    * @param desc
    */
  private def debugInfo(data: DataFrame, desc: String): Unit = {
    println(desc)
    data.printSchema()
    data.show(20, false)
  }

  /**
    * 计算用户
    *
    * @param hiveSqlContext
    * @param statDay
    * @param rawDF
    * @param allUserDF
    */
  private def computeCurrentAllAndNewUser(hiveSqlContext: HiveContext, statDay: String, rawDF: DataFrame, allUserDF: DataFrame): Unit = {
    import hiveSqlContext.implicits._

    // 生成当前的 "所有用户"
    val currentAllUserDF = rawDF.unionAll(allUserDF).distinct()
      .map(r => (r.getAs[String](APPKEY), r.getAs[String](GID), statDay))
      .toDF(APPKEY, GID, DATE)

    debugInfo(currentAllUserDF, "current all user...")

    currentAllUserDF.write.mode(SaveMode.Append).partitionBy(DATE).saveAsTable(Params.hiveAlluserTable)

    // 生成当前的 "新增用户"
    val currentNewUserDF = rawDF.distinct().except(allUserDF)
      .map(r => (r.getAs[String](APPKEY), r.getAs[String](GID), statDay))
      .toDF(APPKEY, GID, DATE)

    debugInfo(currentNewUserDF, "current new user...")

    currentNewUserDF.write.mode(SaveMode.Append).partitionBy(DATE).saveAsTable(Params.hiveNewuserTable)
  }

  /**
    * 保留到 mongodb
    *
    * @param r
    * @param baseDay : 基准日期
    * @param statDay : 要统计的对象日期
    */
  private def saveMongodb(r: DataFrame, baseDay: String, statDay: String): Unit = {
    r.foreachPartition {
      partitionOfRecords =>
        val connection = MongoDBConnection.getInstance
        partitionOfRecords.foreach(
          record => {
            val appkey = record.getAs[String](0)
            val bc = record.getAs[Long](1)
            val rc = record.getAs[Long](2)

            val query = MongoDBObject(APPKEY -> appkey, BASE_DAY -> baseDay, STAT_DAY -> statDay)
            val obj = MongoDBObject(APPKEY -> appkey, BASE_DAY -> baseDay, STAT_DAY -> statDay,
              "bc" -> bc, "rc" -> rc, "ratio" -> rc * 1.0 / bc)

            connection.collConn.update(query, obj, upsert = true)
          }
        )
    }
  }

  def main(args: Array[String]): Unit = {
    // 显示一下参数信息
    logger.info(s"params info: ${Params.toString}")

    // 获取统计的日期, 要计算的日期
    val (statDay, computeDays) = getDays(args)

    logger.info(s"stat date is: ${statDay}")
    logger.info(s"stat days is: ${computeDays.mkString(" ")}")

    if (computeDays.length < 1) {
      logger.error("compute days must be more than or equals to one")
      System.exit(1)
    }

    // 统计日的前一天
    val preStatDay = computeDays(0)

    // 初始化 SparkContext
    val conf = new SparkConf().setAppName("UserRetention")
    val sc = new SparkContext(conf)
    val hiveSqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // 初始化
    init(statDay, hiveSqlContext)

    // 读取 hdfs 中的原始数据
    logger.info(s"hdfs path: ${Params.sourcedataDir}/${statDay}")

    val rawDF0 = hiveSqlContext.read.json(s"${Params.sourcedataDir}/${statDay}").select(APPKEY, GID)
    val rawDF = rawDF0.repartition(Params.sourcedataPartition, rawDF0(APPKEY), rawDF0(GID)).persist(StorageLevel.MEMORY_AND_DISK)

    debugInfo(rawDF, "raw json data...")

    // 读取 hive table 中的所有用户数据
    val sql = s"select ${APPKEY}, ${GID} from ${Params.hiveAlluserTable} where date=${preStatDay}"

    logger.info(s"get all user sql: ${sql}")

    val allUserDF = hiveSqlContext.sql(sql).persist(StorageLevel.MEMORY_AND_DISK)

    debugInfo(allUserDF, "all user table...")

    // 生成当前的 "所有用户" 和 "新用户" 放在 hive table 中
    computeCurrentAllAndNewUser(hiveSqlContext, statDay, rawDF, allUserDF)

    for (d <- computeDays) {
      println(s"compute day: ${d}")

      // 得到新用户的情况
      val baseSQL =
        s"""select ${APPKEY}, ${GID} from ${Params.hiveNewuserTable} where date="${d}""""
      val baseDF = hiveSqlContext.sql(baseSQL)

      debugInfo(baseDF, "baseDF...")

      // 得到留存用户的情况
      val retenDF = baseDF.intersect(rawDF)

      debugInfo(retenDF, "rentenDF...")

      // 计算留存率
      val baseGroupDF = baseDF.groupBy(APPKEY).count()
      val retenGroupDF = retenDF.groupBy(APPKEY).count()

      debugInfo(baseGroupDF, "baseDF group by...")
      debugInfo(retenGroupDF, "retenDF group by...")

      val r = baseGroupDF.join(retenGroupDF, Array(APPKEY)) // inner join

      debugInfo(r, "result...")

      // 保留到 mongodb
      saveMongodb(r, d, statDay)
    }

    sc.stop()
  }
}