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

import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

import scala.collection.mutable.Map
import scala.util.Random

object MapAccumulatorParam extends AccumulatorParam[Map[String, Int]] {
  def addInPlace(t1: Map[String, Int], t2: Map[String, Int]): Map[String, Int] = {
    for ((k, v) <- t2) {
      t1.get(k) match {
        case None => t1(k) = v
        case Some(e) => t1(k) = e + v
      }
    }

    t1
  }

  def zero(initialValue: Map[String, Int]): Map[String, Int] = initialValue
}

object AccumulatorTest2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AccumulatorTest2")
    val ctx = new SparkContext(sparkConf)

    val accum = ctx.accumulator(Map[String, Int](), "My Accumulator2")(MapAccumulatorParam)

    // 抽样 1000000 万次，平均每个数应该在 100 次（随机的）
    ctx.parallelize(1 to 1000000, 10).foreach(i => {
      val r = Random.nextInt(10000)
      if (5000 < r && r <= 5010) {
        accum += Map(r.toString -> 1)
      }
    })

    println(s"accum: ${accum.value}")
  }
}