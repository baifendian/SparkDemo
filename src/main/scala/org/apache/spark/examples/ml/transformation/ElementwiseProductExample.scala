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

import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object ElementwiseProductExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ElementwiseProductExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 建立向量数据，也支持 "稀疏" 向量
    val dataFrame = sqlContext.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    // 将 dataFrame 进行转换，其实是对 dataFrame 和 transformingVector 进行计算，计算方式也是比较直接的，
    // 即对 vector 中的每个元素对应进行乘积计算
    transformer.transform(dataFrame).show()

    sc.stop()
  }
}