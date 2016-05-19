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

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object VectorIndexerExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: [input] [maxCategories]")
      sys.exit(1)
    }

    val input = args(0)
    val maxCategories = args(1) toInt

    val conf = new SparkConf().setAppName("VectorIndexerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.format("libsvm").load(input)

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(maxCategories) // 设置最大的 label 出现次数

    // 对 data 进行拟合
    val indexerModel = indexer.fit(data)

    // 有哪些 feature，具体的是 column
    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet

    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // 建立新的 column "indexed"
    val indexedData = indexerModel.transform(data)

    indexedData.show()

    sc.stop()
  }
}