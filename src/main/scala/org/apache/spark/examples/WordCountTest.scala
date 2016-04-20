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
package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

// 为了测试 spark 内部原理写的一个程序，读者可以忽略之
object WordCountTest {
  def uuid = java.util.UUID.randomUUID.toString

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: <partition> <output-dir>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("WordCountTest")
    val sc = new SparkContext(sparkConf)

    val partition = args(0) toInt
    val outputDir = args(1)

    // 生成大约 1G 的数据，目的就是让单词不重复，这样 map-reduce 端无法进行 combine
    // 注意 reduceByKey 是会在 mapper 端进行 merge 的，类似于 mapreduce 中的 combiner 做的事情
    val wc = sc.parallelize(1 to 30000000, partition).map(_ => (uuid, 1)).reduceByKey(_ + _)

    wc.saveAsTextFile(outputDir)

    sc.stop()
  }
}