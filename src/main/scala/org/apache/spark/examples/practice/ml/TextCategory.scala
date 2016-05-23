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
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

@Experimental("will be use in spark 2.0")
object TextCategory {
  private val logger = Logger.getLogger(getClass.getName)

  private val TITLE = "title"
  private val TITLE_WORDS = "title_words"
  private val QUAN_TITLE_WORDS = "quan_title_words"
  private val MED_QUAN_TITLE_WORDS = "med_quan_title_words"

  private val CATEGORY = "category"
  private val BRAND = "brand"
  private val BRAND_WORDS = "brand_words"
  private val STOPWORD_BRAND_WORDS = "stopword_brand_words"

  private val RAW_FEATURES = "rawFeatures"
  private val FEATURES = "features"

  private val INDEXED_LABEL = "indexedLabel"

  private val PREDICTION = "prediction"
  private val PREDICTED_LABEL = "predictedLabel"

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

  // 数据预处理
  private def preProcess(parser: ConfigParser, data: DataFrame): DataFrame = {
    // 对标题进行中文分词
    val tokenizer = new ChineseSegment(Option(parser.userDict)).
      setInputCol(TITLE).
      setOutputCol(TITLE_WORDS)

    // 处理单位词信息, 之后对数词也做一下过滤
    val quantifier = new QuantifierProcess(parser.preprocessQuantifierFile).
      setInputCol(TITLE_WORDS).
      setOutputCol(QUAN_TITLE_WORDS)

    // 对药品词信息做一下处理
    val medicine = new MedicineProcess(parser.preprocessMedicineFile).
      setInputCol(QUAN_TITLE_WORDS).
      setOutputCol(MED_QUAN_TITLE_WORDS)

    // 进行转换处理
    medicine.transform(quantifier.transform(tokenizer.transform(data)))
  }

  // 构造 pipeline
  private def featurePipeline(parser: ConfigParser, sqlContext: SQLContext): Pipeline = {
    // 将 brand 和 title_words 融合
    val sqlTrans = new SQLTransformer().setStatement(
      s"SELECT *, strArrayMerge(${BRAND}, ${MED_QUAN_TITLE_WORDS}) AS ${BRAND_WORDS} FROM __THIS__")

    // 停用词过滤
    val remover = new StopWordsRemover()
      .setInputCol(BRAND_WORDS)
      .setOutputCol(STOPWORD_BRAND_WORDS)
      .setStopWords(loadStopWords(parser.preprocessStopFile))

    // 计算 TF
    val hashingTF = new HashingTF()
      .setInputCol(STOPWORD_BRAND_WORDS)
      .setOutputCol(RAW_FEATURES)
      .setNumFeatures(parser.commonTFNumber)

    // 构建向量(对 word 的形式构建 tf-idf, 对于 topic 的形式, 采用 lda 处理)
    val space = parser.commonVecSpace match {
      case "word" =>
        new IDF()
          .setInputCol(RAW_FEATURES)
          .setOutputCol(FEATURES)
      case _ => /*new LDA()
        .setK(parser.topicParamNTopics)
        .setMaxIter(parser.topicParamNIter)
        .setFeaturesCol("rawFeatures")
        .setTopicDistributionCol("features")*/
        new Word2Vec()
          .setInputCol(RAW_FEATURES)
          .setOutputCol(FEATURES)
          .setVectorSize(parser.topicParamWord2vecSize)
          .setMinCount(0)
    }

    new Pipeline().setStages(Array(sqlTrans, remover, hashingTF, space))
  }

  /**
    * 训练模型, 训练之后保存到指定的目录
    *
    */
  def train: Unit = {
    // 加载配置
    val parser: ConfigParser = ConfigParser()

    logger.info(s"config info: ${parser.toString}")

    // 初始化 SparkContext
    val conf = new SparkConf().setAppName("TextCategory")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 自定义函数, 注意 array 类型为 Seq, 不能为 Array
    sqlContext.udf.register("strArrayMerge", (str: String, array: Seq[String]) => str +: array)

    // 加载数据
    val trainingDataFrame = sqlContext.read.json(parser.trainPath)
    trainingDataFrame.printSchema()

    // 数据预处理
    val preDataFrame = preProcess(parser, trainingDataFrame)
    preDataFrame.printSchema()

    // 数据先进行转换, 对分类进行转换
    val labelIndexer = new StringIndexer()
      .setInputCol(CATEGORY)
      .setOutputCol(INDEXED_LABEL)

    logger.info("index model training and save")
    val labelModel = labelIndexer.fit(preDataFrame)
    val labelDataFrame = labelModel.transform(preDataFrame)

    labelModel.write.overwrite().save(parser.commonIndexModelPath)

    // 构造 feature 的 pipe
    logger.info("construct pipeline")
    val pipeline = featurePipeline(parser, sqlContext)

    logger.info("model training and save")
    val featureModel = pipeline.fit(labelDataFrame)
    featureModel.write.overwrite().save(parser.commonModelPath)

    // 模型训练过程
    val rf = new RandomForestClassifier()
      .setLabelCol(INDEXED_LABEL)
      .setFeaturesCol(FEATURES)
      .setNumTrees(parser.trainForestNum)

    // 逆转换过程
    val labelConverter = new IndexToString()
      .setInputCol(PREDICTION)
      .setOutputCol(PREDICTED_LABEL)
      .setLabels(labelModel.labels)

    val model = rf.fit(labelDataFrame)

    // 模型效果测试
    val predictions = labelConverter.transform(model.transform(labelDataFrame))

    predictions.printSchema()
    predictions.show(20)

    // 评估分类的效果
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(INDEXED_LABEL)
      .setPredictionCol(PREDICTION)
      .setMetricName("precision")

    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    // spark context stop
    sc.stop()
  }

  /**
    * 测试模型
    *
    */
  def test: Unit = {
    // 加载配置
    val parser: ConfigParser = ConfigParser()

    // 初始化
    val conf = new SparkConf().setAppName("TextCategory")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 自定义函数, 注意 array 类型为 Seq, 不能为 Array
    sqlContext.udf.register("strArrayMerge", (str: String, array: Seq[String]) => str +: array)

    // 读取待测试数据
    val testDataFrame = sqlContext.read.json(parser.testFilePath)
    testDataFrame.printSchema()

    // 数据预处理
    val preDataFrame = preProcess(parser, testDataFrame)

    // 加载 index 模型, 分类模型
    val indexModel = PipelineModel.load(parser.commonIndexModelPath)
    val model = PipelineModel.load(parser.commonModelPath)

    // 进行实际的测试
    val labelDataFrame = indexModel.transform(preDataFrame)
    val testResult = model.transform(labelDataFrame)

    testResult.printSchema()
    testResult.show(20)

    // 查看分类效果
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")

    val accuracy = evaluator.evaluate(testResult)
    println("Test Error = " + (1.0 - accuracy))

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: [train/predict/test]")
      System.exit(1)
    }

    args(0) match {
      case "train" => train
      case "test" => test
      case "predict" => Prediction.predict
      case _ => logger.error("invalid command, must be train or predict.")
    }
  }
}