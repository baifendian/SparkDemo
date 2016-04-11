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

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Set
import scala.io.Source

object BroadcastTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <local-dict> <hadoop-file>")
      System.exit(1)
    }

    // 加载词典，并且设置为 "广播变量"
    val sources = Source.fromFile(args(0))
    val dict = Set[String]()

    for (line <- sources.getLines()) {
      dict += line
    }

    val sparkConf = new SparkConf().setAppName("BroadcastTest")
    val ctx = new SparkContext(sparkConf)
    val broadcastVar = ctx.broadcast(dict)

    // 加载 hadoop file，判断是否包含特定的词典
    val sentences = ctx.textFile(args(1)).flatMap(x => x.split("\\."))

    val rdd = sentences.filter(x => {
      val words = x.split("\\s+")
      words.exists(x => broadcastVar.value.contains(x))
    })

    rdd.collect.foreach(println)

    ctx.stop()
  }
}