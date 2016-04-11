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

import scala.util.Random

object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AccumulatorTest")
    val ctx = new SparkContext(sparkConf)

    val accum = ctx.accumulator(0, "My Accumulator")

    ctx.parallelize(1 to 1000000, 10).foreach(i => {
      val r = Random.nextInt(10000)
      if (5000 < r && r <= 5050) {
        accum += 1
      }
    })

    println(s"accum: ${accum.value}, ")
  }
}