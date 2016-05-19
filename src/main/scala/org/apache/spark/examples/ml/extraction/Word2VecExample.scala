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
package org.apache.spark.examples.ml.extraction

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Word2VecExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word2Vec")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" "),
      "mapreduce spark".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(5) // 每篇 document 会映射为一个 vector，这里设置了维度
      .setMinCount(0)

    val model = word2Vec.fit(documentDF)

    // 对每个 document 进行一个 transform
    val result = model.transform(documentDF)

    result.select("result").take(4).foreach(println)

    sc.stop()
  }
}