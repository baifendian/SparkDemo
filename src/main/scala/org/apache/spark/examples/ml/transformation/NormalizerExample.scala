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
package org.apache.spark.examples.ml.transformation

import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object NormalizerExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: [input]")
      sys.exit(1)
    }

    val input = args(0)

    val conf = new SparkConf().setAppName("NormalizerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Normalize each Vector using $L^1$ norm.
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)

    // 显示 transform 之后的结果
    l1NormData.show()

    // 正无穷大，相当于 value/maxValue
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    lInfNormData.show()

    sc.stop()
  }
}