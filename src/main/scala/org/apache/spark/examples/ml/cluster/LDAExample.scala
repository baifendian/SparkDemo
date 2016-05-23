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

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object LDAExample {
  val FEATURES_COL = "features"

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <input>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("LDAExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val input = args(0)

    // Loads data
    val rowRDD = sc.textFile(input).filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))

    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT, false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)

    dataset.printSchema()
    dataset.show(20)

    // Trains a LDA model
    val lda = new LDA()
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)

    val model = lda.fit(dataset)
    val transformed = model.transform(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    // describeTopics
    val topics = model.describeTopics(3)

    // Shows the result
    topics.show(false)
    transformed.show(false)

    sc.stop()
  }
}