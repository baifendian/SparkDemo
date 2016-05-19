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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.{Tokenizer, RegexTokenizer}

object TokenizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TokenizerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val sentenceDataFrame = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("label", "sentence")

    // 采用空格分词
    val tokenizer = new Tokenizer().
      setInputCol("sentence").
      setOutputCol("words")

    // \W 是用来匹配任意不是字母，数字，下划线，汉字的字符
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val tokenized = tokenizer.transform(sentenceDataFrame)

    println("result of tokenizer...")
    tokenized.select("words", "label").take(3).foreach(println)

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)

    println("result of regex tokenizer...")
    regexTokenized.select("words", "label").take(3).foreach(println)

    sc.stop()
  }
}