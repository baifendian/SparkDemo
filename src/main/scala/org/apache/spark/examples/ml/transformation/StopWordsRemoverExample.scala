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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SQLContext

object StopWordsRemoverExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StopWordsRemoverExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    println("current stop words:")
    println(remover.getStopWords)

    val dataSet = sqlContext.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    println("stop words remove")
    remover.transform(dataSet).show()

    println("stop words remove after add some words")
    remover.setStopWords(remover.getStopWords ++ Array[String]("red"))
    remover.transform(dataSet).show()

    sc.stop()
  }
}