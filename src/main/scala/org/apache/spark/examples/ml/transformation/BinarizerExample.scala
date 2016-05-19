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

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object BinarizerExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <threshold>")
      System.exit(1)
    }

    val threshold = args(0) toDouble

    val conf = new SparkConf().setAppName("BinarizerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 注意这里的 create data frame，接收的是 array 类型
    val data = Array(
      (0, 0.1),
      (1, 0.8),
      (2, 0.3))

    val dataFrame: DataFrame = sqlContext.createDataFrame(data).toDF("label", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(threshold)

    val binarizedDataFrame = binarizer.transform(dataFrame)
    val binarizedFeatures = binarizedDataFrame.select("binarized_feature")

    binarizedFeatures.collect().foreach(println)

    sc.stop()
  }
}