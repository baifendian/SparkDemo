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

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

// 为了测试 spark 内部原理写的一个程序，读者可以忽略之
object GroupTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: <numMappers> <numKVPairs> <valSize> <numReducers>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("GroupTest")
    val sc = new SparkContext(sparkConf)

    val numMappers = args(0) toInt // mapper 数
    val numKVPairs = args(1) toInt // 很对每个 key，其 key-value pair 的数目
    val valSize = args(2) toInt // value 大小
    val numReducers = args(3) toInt // 每个 reducer 的数目

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }

      arr1
    }.cache

    println(pairs1.count)
    println(pairs1.groupByKey(numReducers).count)

    sc.stop()
  }
}