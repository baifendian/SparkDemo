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
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest, RandomForestClassifier}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, _}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TextCategoryV16 {
  private val logger = Logger.getLogger(getClass.getName)

  private val ID = "id"

  private val TITLE = "title"
  private val TITLE_WORDS = "title_words"
  private val QUAN_TITLE_WORDS = "quan_title_words"
  private val MED_QUAN_TITLE_WORDS = "med_quan_title_words"

  private val CATEGORY = "category"
  private val BRAND = "brand"
  private val BRAND_WORDS = "brand_words"
  private val STOPWORD_BRAND_WORDS = "stopword_brand_words"

  private val RAW_FEATURES = "raw_features"
  private val IDF_FEATURES = "idf_features"
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

    // 很多数据带了 "None", "NULL", " ", 需要把它去掉
    words += "None"
    words += "NULL"
    words += " "
    words += "\t"

    println(s"load stop words: $count")

    words toArray
  }

  // 构造 pipeline
  private def constructPipeline(parser: ConfigParser, sqlContext: SQLContext, data: Option[DataFrame] = None): Pipeline = {
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
          .setOutputCol(IDF_FEATURES)
      case "topic" =>
        new LDA()
          .setK(parser.topicParamTopicNTopics)
          .setMaxIter(parser.topicParamTopicNIter)
          .setFeaturesCol(RAW_FEATURES)
          .setTopicDistributionCol(FEATURES)
      case _ =>
        new Word2Vec()
          .setInputCol(STOPWORD_BRAND_WORDS)
          .setOutputCol(FEATURES)
          .setVectorSize(parser.topicParamWord2vecSize)
          .setMaxIter(parser.topicParamWord2vecNIter)
          .setNumPartitions(parser.topicParamWord2vecNPartition)
          .setWindowSize(parser.topicParamWord2vecWindowSize)
          .setMinCount(parser.topicParamWord2vecMinCount)
    }

    // 模型训练过程
    val alg = parser.commonAlg match {
      case "rf" =>
        new RandomForestClassifier()
          .setLabelCol(INDEXED_LABEL)
          .setFeaturesCol(FEATURES)
          .setNumTrees(parser.rfTreesNum)
      case _ => {
        val c = new LogisticRegression()
          .setRegParam(parser.lrRegParam)
          .setElasticNetParam(parser.lrElasticNetParam)
          .setMaxIter(parser.lrNIter)
        new OneVsRest()
          .setLabelCol(INDEXED_LABEL)
          .setFeaturesCol(FEATURES)
          .setClassifier(c)
      }
    }

    parser.commonVecSpace match {
      case c: String if (c == "word" || c == "topic") => {
        if (data != None) {
          val pl = new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans,
            remover, hashingTF))
          println("training data debug information")
          pl.fit(data.get).transform(data.get).show(100, false)
        }
        if (c == "topic") {
          new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans,
            remover, hashingTF, space, alg))
        }
        else {
          val selector = new ChiSqSelector()
            .setNumTopFeatures(parser.tfidfFeaturesSelect)
            .setFeaturesCol(IDF_FEATURES)
            .setLabelCol(INDEXED_LABEL)
            .setOutputCol(FEATURES)
          new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans,
            remover, hashingTF, space, selector, alg))
        }
      }
      case _ => {
        if (data != None) {
          val pl = new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans,
            remover))
          println("training data debug information")
          pl.fit(data.get).transform(data.get).show(100, false)
        }
        new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans,
          remover, space, alg))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // 是不是 debug 模式
    val debug = args.length match {
      case len: Int if len > 0 => args(0) toBoolean
      case _ => false
    }

    println(s"debug is: $debug")

    // 加载配置
    val parser: ConfigParser = ConfigParser()

    println(s"config info: ${parser.toString}")

    // 初始化 SparkContext
    val conf = new SparkConf().setAppName("TextCategoryV16")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 自定义函数, 注意 array 类型为 Seq, 不能为 Array, 之所以对 str 做切分, 是因为可能包含空格
    sqlContext.udf.register("strArrayMerge", (str: String, array: Seq[String]) => str.split(" ") ++ array)

    // 数据先进行转换, 对分类进行转换
    val labelIndexer = new StringIndexer()
      .setInputCol(CATEGORY)
      .setOutputCol(INDEXED_LABEL)

    // 加载训练数据
    val rawDataFrame = sqlContext.read.json(parser.trainPath)

    // 如果是 topic 模型, 需要对文本做一个 merge
    val rawTrainingDataFrame = parser.commonNMerge match {
      case x: Int if x > 1 => {
        val m: Int = parser.commonNMerge
        val partition: Int = parser.trainNPartition
        rawDataFrame
          .repartition(partition)
          .map(x => (x.getAs[String]("category"), (x.getAs[String]("brand"), x.getAs[String]("title")))) // 获取类目, 品牌, 标题
          .groupByKey() // 根据类目进行 groupby
          .flatMap({ case (key, value) =>
          for (w <- value.sliding(m, m)) yield (key, w)
        }) // 对 品牌和标题 进行一定的切分
          .map({ case (key, arrayValue) =>
          val brands = ArrayBuffer[String]()
          val titles = ArrayBuffer[String]()
          for ((brand, title) <- arrayValue) {
            brands += brand
            titles += title
          }
          (key, brands.mkString(" "), titles.mkString(" "))
        }) // 把品牌,标题放在一起
          .toDF("category", "brand", "title")
      }
      case _ => rawDataFrame
    }

    val labelModel = labelIndexer.fit(rawTrainingDataFrame)
    val trainingDataFrame = labelModel.transform(rawTrainingDataFrame).repartition(parser.trainNPartition)

    println("training data schema")
    trainingDataFrame.printSchema()

    // 加载测试数据
    val rawTestDataFrame = sqlContext.read.json(parser.testFilePath)
    val testDataFrame = labelModel.transform(rawTestDataFrame)

    println("test data schema")
    testDataFrame.printSchema()

    // 构造 pipeline, 训练模型
    val pipeline = constructPipeline(parser, sqlContext, if (debug == true) Some(trainingDataFrame) else None)

    println("model train")
    val model = pipeline.fit(trainingDataFrame)

    // 测试
    println("model test")
    val rawTestResult: DataFrame = model.transform(testDataFrame)

    // 逆转换过程
    val labelConverter = new IndexToString()
      .setInputCol(PREDICTION)
      .setOutputCol(PREDICTED_LABEL)
      .setLabels(labelModel.labels)

    val testResult = labelConverter.transform(rawTestResult)

    println("test result schema")
    testResult.printSchema()
    println("test result show")
    testResult.show(100, false)

    // 保存, 有些场景下可能不想保存
    if (parser.testResultSave) {
      println("test result save")
      testResult.select(ID, BRAND, TITLE, CATEGORY, PREDICTED_LABEL).write.format("json").save(parser.testResultPath)
    }

    // 评估分类的效果
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(INDEXED_LABEL)
      .setPredictionCol(PREDICTION)
      .setMetricName("precision")

    println("model evaluation")
    val accuracy = evaluator.evaluate(testResult)

    println("Test Error = " + (1.0 - accuracy))

    // 对训练数据也做一个测试, 看看有没有过拟合现象
    val trainRawTestResult: DataFrame = model.transform(labelModel.transform(rawDataFrame))
    val trainResult = labelConverter.transform(trainRawTestResult)

    println("Test Error on training data = " + (1.0 - evaluator.evaluate(trainResult)))

    // spark context stop
    sc.stop()
  }
}