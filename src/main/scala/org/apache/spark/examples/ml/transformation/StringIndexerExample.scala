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

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object StringIndexerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StringIndexerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 创建 df
    val df = sqlContext.createDataFrame(
      Seq((0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    // 对 category 字段进行编码，fit 相当于是训练拟合的过程，transform 是一个转换的过程
    val indexed = indexer.fit(df).transform(df)

    // 显示转换后的结果
    indexed.show()

    sc.stop()
  }
}