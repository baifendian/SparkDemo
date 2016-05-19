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
package org.apache.spark.examples.ml.cluster

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object KMeansExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Crates a DataFrame
    val dataset: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 0.0)),
      (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)),
      (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)),
      (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("id", "features")

    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(2)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    // 生成模型
    val model = kmeans.fit(dataset)

    // Shows the result
    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    println("Show cluster results: ")
    model.transform(dataset).show()

    sc.stop()
  }
}