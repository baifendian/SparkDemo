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

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SaveJDBCTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage: <conn-url> <file-for-read> <table-for-write> <user> <passwd>")
      System.exit(1)
    }

    val url = args(0)
    val file = args(1)
    val table = args(2)
    val user = args(3)
    val passwd = args(4)

    val conf = new SparkConf().setAppName("SaveJDBCTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.textFile(file).map(_.split( """\s+""")).map(p => Weather(p(0), p(1), p(2).toInt, p(3).toInt)).toDF

    val props = new Properties()
    props.put("user", user)
    props.put("password", passwd)

    df.write.jdbc(url, table, props)

    sc.stop()
  }
}