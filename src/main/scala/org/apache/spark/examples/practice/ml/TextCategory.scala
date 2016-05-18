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
package org.apache.spark.examples.practice.ml

import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{StopWordsRemover, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TextCategory {
  val logger = Logger.getLogger(getClass.getName)

  def loadStopWords(stopwordFile: String): Array[String] = {
    val br: BufferedReader = Files.newBufferedReader(Paths.get(stopwordFile), StandardCharsets.UTF_8)
    val words = ArrayBuffer[String]()

    var count = 0

    while (br.ready()) {
      words += br.readLine()
      count += 1
    }

    println(s"load stop words: $count")

    words toArray
  }

  /**
    * 训练模型, 训练之后保存到指定的目录
    *
    */
  def train: Unit = {
    // === 加载配置 ===
    val parser: ConfigParser = ConfigParser()

    logger.info(s"config info: ${parser.toString}")

    // === 初始化 SparkContext ===
    val conf = new SparkConf().setAppName("TextCategory")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val trainingDataFrame = sqlContext.read.json(parser.trainPath)
    trainingDataFrame.printSchema()

    // === 预处理过程 ===
    // 对标题进行中文分词
    val tokenizer = new ChineseSegment(Option(parser.userDict)).
      setInputCol("title").
      setOutputCol("title_words")

    val tokenized = tokenizer.transform(trainingDataFrame)
    tokenized.select("id", "brand", "title_words").take(50).foreach(x => println(s"tokenizer result: $x"))

    // 处理单位词信息, 之后对数词也做一下过滤
    val quantifier = new QuantifierProcess(parser.preprocessQuantifierFile).
      setInputCol("title_words").
      setOutputCol("quan_title_words")

    val quantified = quantifier.transform(tokenized)
    quantified.select("id", "brand", "quan_title_words").take(50).foreach(x => println(s"quantifier result: $x"))

    // 对药品词信息做一下处理
    val medicine = new MedicineProcess(parser.preprocessMedicineFile).
      setInputCol("quan_title_words").
      setOutputCol("med_quan_title_words")

    val medicined = medicine.transform(quantified)
    medicined.select("id", "brand", "med_quan_title_words").take(50).foreach(x => println(s"med result: $x"))

    // 停用词过滤
    val remover = new StopWordsRemover()
      .setInputCol("med_quan_title_words")
      .setOutputCol("stopword_med_quan_title_words")
      .setStopWords(loadStopWords(parser.preprocessStopFile))

    val removered = remover.transform(medicined)
    removered.select("id", "brand", "stopword_med_quan_title_words").take(50).foreach(x => println(s"stop words result: $x"))

    // 将 brand 和 title_words 融合
    val assembler = new VectorAssembler()
      .setInputCols(Array("brand", "stopword_med_quan_title_words"))
      .setOutputCol("brand_words")

    val combined = assembler.transform(removered)
    combined.select("id", "brand_words").take(50).foreach(x => println(s"combined result: $x"))

    // === 构建向量(对 word 的形式构建 tf-idf, 对于 topic 的形式, 采用 lda 处理) ===

    // === 构建 Pipeline ===

    // === 训练模型 ===

    sc.stop()
  }

  /**
    * 预测过程, 从 kafka 读取数据进行预测, 预测的结果写到 redis 中
    */
  def predict: Unit = {

  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: [train/predict]")
      System.exit(1)
    }

    args(0) match {
      case "train" => train
      case "predict" => predict
      case _ => logger.error("invalid command, must be train or predict.")
    }
  }
}