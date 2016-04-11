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

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveOperationTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <inpath>")
      System.exit(1)
    }

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("HiveOperationTest")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // create table
    sqlContext.sql("CREATE TABLE IF NOT EXISTS weather (date STRING, city STRING, minTem Int, maxTem Int) row format delimited fields terminated by '\t'")
    sqlContext.sql(s"LOAD DATA INPATH '${inputFile}' INTO TABLE weather")

    // Queries are expressed in HiveQL
    sqlContext.sql("select city, avg(minTem) from weather group by city").collect().foreach(println)

    // 使用 udf
    sqlContext.udf.register("class", (s: Int) => if (s <= 20) "lower" else "high")

    sqlContext.sql("select city, maxTem, class(maxTem) from weather").collect().foreach(println)

    sc.stop()
  }
}