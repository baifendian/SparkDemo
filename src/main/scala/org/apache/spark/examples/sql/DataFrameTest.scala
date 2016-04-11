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
package org.apache.spark.examples.sql

import java.io.File

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: <input-file> <output-dir>")
      System.exit(1)
    }

    val inputFile = args(0)
    val outputDir = args(1)

    val conf = new SparkConf().setAppName("DataFrameTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.textFile(inputFile).map(_.split( """\s+""")).map(p => Weather(p(0), p(1), p(2).toInt, p(3).toInt)).toDF

    // print schema
    df.printSchema()

    // number of rows
    println("Count: " + df.count())

    // first row
    println("First row: " + df.first())

    // Displays the top 20 rows
    df.show()

    println("Filter city beijing")
    df.filter(df("city") === "北京").show()

    println("Group by city")
    df.groupBy("city").count().show()

    println("Group by city, avg minTem")
    df.groupBy("city").avg("minTem").show()

    df.registerTempTable("weather")

    println("Group by city, avg minTem throw sql")
    sqlContext.sql("SELECT city, avg(minTem) FROM weather group by city").show()

    // 注意是写到了 json 目录中，而非是单个文件
    println("Write to: " + outputDir + File.separator + "json")

    df.write.mode(SaveMode.Overwrite).json(outputDir + File.separator + "json")

    // 实现一个 udf
    sqlContext.udf.register("class", (s: Int) => if (s <= 20) "lower" else "high")

    sqlContext.sql("select city, maxTem, class(maxTem) from weather").show()

    // 测试一下 partition，注意会在 country=china 中写多个文件
    val filePath = outputDir + File.separator + "partition"

    println("Partition file: " + filePath + File.separator + "country=china")

    df.write.mode(SaveMode.Overwrite).json(filePath + File.separator + "country=china")

    println("Partition schema")

    sqlContext.read.json(filePath).printSchema()

    sc.stop()
  }
}