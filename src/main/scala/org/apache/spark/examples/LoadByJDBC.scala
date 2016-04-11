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
package org.apache.spark.examples

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object LoadByJDBC {
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    DriverManager.getConnection("jdbc:mysql://172.19.1.76/bma_local?", "root", "123456")
  }

  def extractValues(r: ResultSet) = {
    (r.getString(5), r.getDate(3))
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LoadByJDBC")

    val sc = new SparkContext(sparkConf)

    // JdbcRDD 的第二个参数是 sql，比较有趣: 该 query 必须包含两个 ? 占位符，用于对结果进行 partition.
    // 最后一个参数是 mapRow: 这个函数是完成从 ResultSet 到单个 row 的转换，只需要调用 getInt, getString, 等等; 默认 function 是将一个 ResultSet 转化为 Object array.
    val data = new JdbcRDD(sc,
      createConnection, "SELECT * FROM auth_user WHERE ? <= id AND ID <= ?",
      lowerBound = 1, upperBound = 100, numPartitions = 3, mapRow = extractValues)

    println(data.collect().toList)

    sc.stop()
  }
}