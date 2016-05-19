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

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object NGramExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NGramExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val wordDataFrame = sqlContext.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val ngram = new NGram().
      setInputCol("words").
      setOutputCol("ngrams").
      setN(3) // 设置 n-gram 的 n 为 3

    val ngramDataFrame = ngram.transform(wordDataFrame)

    ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)

    sc.stop()
  }
}