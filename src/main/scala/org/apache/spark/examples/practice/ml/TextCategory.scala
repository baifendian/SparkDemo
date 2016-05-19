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
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TextCategory {
  val logger = Logger.getLogger(getClass.getName)

  private def loadStopWords(stopwordFile: String): Array[String] = {
    val br: BufferedReader = Files.newBufferedReader(Paths.get(stopwordFile), StandardCharsets.UTF_8)
    val words = ArrayBuffer[String]()

    var count = 0

    while (br.ready()) {
      words += br.readLine()
      count += 1
    }

    // 很多数据带了 "None", 需要把它去掉
    words += "None"
    words += "NULL"
    words + " "

    println(s"load stop words: $count")

    words toArray
  }

  // 预处理过程
  private def preprocess(parser: ConfigParser, sqlContext: SQLContext, data: DataFrame, isTraining: Boolean): DataFrame = {
    // 对标题进行中文分词
    val tokenizer = new ChineseSegment(Option(parser.userDict)).
      setInputCol("title").
      setOutputCol("title_words")

    val tokenized = tokenizer.transform(data)
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

    // 将 brand 和 title_words 融合
    val sqlTrans = isTraining match {
      case true => new SQLTransformer().setStatement(
        "SELECT id, category, strArrayMerge(brand, med_quan_title_words) AS brand_words FROM __THIS__")
      case false => new SQLTransformer().setStatement(
        "SELECT id, strArrayMerge(brand, med_quan_title_words) AS brand_words FROM __THIS__")
    }

    val merged = sqlTrans.transform(medicined)
    merged.select("id", "brand_words").take(50).foreach(x => println(s"merge brand and title result: $x"))

    // 停用词过滤
    val remover = new StopWordsRemover()
      .setInputCol("brand_words")
      .setOutputCol("stopword_brand_words")
      .setStopWords(loadStopWords(parser.preprocessStopFile))

    val removered = remover.transform(merged)
    removered.select("id", "stopword_brand_words").take(50).foreach(x => println(s"stop words result: $x"))

    // 构建向量(对 word 的形式构建 tf-idf, 对于 topic 的形式, 采用 lda 处理)
    parser.commonVecSpace match {
      case "word" => {
        val hashingTF = new HashingTF()
          .setInputCol("stopword_brand_words")
          .setOutputCol("rawFeatures")
          .setNumFeatures(parser.commonTFNumber)

        val featurizedData = hashingTF.transform(removered)
        featurizedData.select("id", "rawFeatures").take(50).foreach(x => println(s"raw features result: $x"))

        val idf = new IDF()
          .setInputCol("rawFeatures")
          .setOutputCol("features")

        logger.info("before idf transform...")

        val idfModel = idf.fit(featurizedData)
        val rescaledData = idfModel.transform(featurizedData)

        rescaledData.select("id", "features").take(50).foreach(x => println(s"features result: $x"))

        logger.info("end idf transform")

        val pipeline = new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans, remover, hashingTF, idf))
        val model = pipeline.fit(data)
        val pipelineData = model.transform(data)

        pipelineData
      }
      case _ => {
        val hashingTF = new HashingTF()
          .setInputCol("stopword_brand_words")
          .setOutputCol("rawFeatures")
          .setNumFeatures(parser.commonTFNumber)

        val featurizedData = hashingTF.transform(removered)
        featurizedData.select("id", "rawFeatures").take(50).foreach(x => println(s"raw features result: $x"))

        val lda = new LDA()
          .setK(parser.topicParamNTopics)
          .setMaxIter(parser.topicParamNIter)
          .setFeaturesCol("rawFeatures")
          .setTopicDistributionCol("features")

        logger.info("before lda transform...")

        val ldaModel = lda.fit(featurizedData)
        val transformed = ldaModel.transform(featurizedData)

        // describeTopics
        val topics = ldaModel.describeTopics(3)

        // Shows the result
        println("show topics...")
        topics.show(false)

        println("show transformed...")
        transformed.show(false)

        logger.info("end lda transform")

        val pipeline = new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans, remover, hashingTF, ldaModel))
        val model = pipeline.fit(data)
        val pipelineData = model.transform(data)

        pipelineData
      }
    }
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

    // 自定义函数, 注意 array 类型为 Seq, 不能为 Array
    sqlContext.udf.register("strArrayMerge", (str: String, array: Seq[String]) => str +: array)

    val trainingDataFrame = sqlContext.read.json(parser.trainPath)
    trainingDataFrame.printSchema()

    // 进行预处理
    val data = preprocess(parser, sqlContext, trainingDataFrame, true)
    data.select("id", "features").take(50).foreach(x => println(s"pipeline result: $x"))

    // === 构建 Pipeline ===
    val labelIndexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("indexedLabel")
      .fit(data)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setNumTrees(parser.trainForestNum)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, rf, labelConverter))

    // === 训练模型 ===
    val model = pipeline.fit(data)

    // 保存模型文件
    model.save(parser.commonModelPath)

    // === 进行预测 ===
    val predictions = model.transform(data)

    predictions.select("id", "category", "predictedLabel", "features").show(5)

    // === 查看分类效果 ===
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")

    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    // spark context stop
    sc.stop()
  }

  private def functionToCreateContext(): StreamingContext = {
    // 加载配置文件, 配置文件示例为: conf.properties
    val sparkConf = new SparkConf().setAppName("Kafka2Hdfs").
      set("spark.streaming.receiver.writeAheadLog.enable", "true"). // 先写日志, 提高容错性, 避免 receiver 挂掉
      set("spark.streaming.receiver.maxRate", "5000"). // 每秒的读取速率
      set("spark.streaming.stopGracefullyOnShutdown", "true"). // 设置为 true 会 gracefully 的关闭 StreamingContext
      set("spark.streaming.blockInterval", "1000ms") // block 的大小, 每个 block interval 的数据对应于一个 task

    // 创建 spark context 和 streaming context, 注意这里也设置了 checkpoint, 目的用于 stream 的状态恢复
    val ctx = new SparkContext(sparkConf)
    val ssc = new StreamingContext(ctx, Seconds(10))

    // check point 特性不是很稳定, 慎用!!!
    ssc.checkpoint("checkpoint/TextCategory")

    ssc
  }

  /**
    * 预测模型
    *
    */
  def predict: Unit = {
    // === 加载配置 ===
    val parser: ConfigParser = ConfigParser()

    val ssc = StreamingContext.getOrCreate("checkpoint/TextCategory", functionToCreateContext _)
    val ctx = ssc.sparkContext
    val sqlContext = new SQLContext(ctx)

    // 自定义函数, 注意 array 类型为 Seq, 不能为 Array
    sqlContext.udf.register("strArrayMerge", (str: String, array: Seq[String]) => str +: array)

    val model = PipelineModel.load(parser.commonModelPath)

    // 读取流数据, 解析, 采用模型预测
    val kafkaStreams = (1 to parser.predictNumStreams).map { i => {
      val topicMap = parser.predictTopics.split(",").map((_, 1)).toMap
      KafkaUtils.createStream(ssc, parser.predictZookeeper, parser.predictGroupid, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    }
    }

    // message 进行 union, 注意我们并没有进行 repartition, 这是因为上面我们用了 "blockInterval"
    val messages = ssc.union(kafkaStreams)

    // 预测结果写入到 redis 中


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