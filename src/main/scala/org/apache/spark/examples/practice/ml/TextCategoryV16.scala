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
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes, OneVsRest, RandomForestClassifier}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, _}
import org.apache.spark.ml.{Pipeline, PipelineStage}
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

  private def checkParams: Boolean = {
    // w2v 计算的结果, nb 是不识别的
    if (Params.commonVecSpace == "w2v" && Params.commonAlg == "nb") false
    else true
  }

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
  private def constructPipeline(sqlContext: SQLContext, data: Option[DataFrame] = None): Pipeline = {
    val stages = ArrayBuffer[PipelineStage]()

    // 对标题进行中文分词
    val tokenizer = new ChineseSegment(Option(Params.userDict)).
      setInputCol(TITLE).
      setOutputCol(TITLE_WORDS)

    // 处理单位词信息, 之后对数词也做一下过滤
    val quantifier = new QuantifierProcess(Params.preprocessQuantifierFile).
      setInputCol(TITLE_WORDS).
      setOutputCol(QUAN_TITLE_WORDS)

    // 对药品词信息做一下处理
    val medicine = new MedicineProcess(Params.preprocessMedicineFile).
      setInputCol(QUAN_TITLE_WORDS).
      setOutputCol(MED_QUAN_TITLE_WORDS)

    // 将 brand 和 title_words 融合
    val sqlTrans = new SQLTransformer().setStatement(
      s"SELECT *, strArrayMerge(${BRAND}, ${MED_QUAN_TITLE_WORDS}) AS ${BRAND_WORDS} FROM __THIS__")

    // 停用词过滤
    val remover = new StopWordsRemover()
      .setInputCol(BRAND_WORDS)
      .setOutputCol(STOPWORD_BRAND_WORDS)
      .setStopWords(loadStopWords(Params.preprocessStopFile))

    stages +=(tokenizer, quantifier, medicine, sqlTrans, remover)

    // 计算 TF
    val tf = Params.commonTF match {
      case "hash" =>
        new HashingTF()
          .setInputCol(STOPWORD_BRAND_WORDS)
          .setOutputCol(RAW_FEATURES)
          .setNumFeatures(Params.hashTFNumber)
      case "countvec" =>
        new CountVectorizer()
          .setInputCol(STOPWORD_BRAND_WORDS)
          .setOutputCol(RAW_FEATURES)
          .setVocabSize(Params.countvecVocabSize)
          .setMinDF(Params.countvecMinDF)
      case _ => {
        throw new Exception(s"tf not valid: ${Params.commonTF}")
      }
    }

    // 构建向量空间
    val space = Params.commonVecSpace match {
      case "word" =>
        stages +=(tf,
          new IDF()
            .setInputCol(RAW_FEATURES)
            .setOutputCol(FEATURES)
          /*
          new IDF()
            .setInputCol(RAW_FEATURES)
            .setOutputCol(IDF_FEATURES),
          new ChiSqSelector()
            .setFeaturesCol(IDF_FEATURES)
            .setLabelCol(INDEXED_LABEL)
            .setOutputCol(FEATURES)
            .setNumTopFeatures(Params.tfidfFeaturesSelect)*/
          )
      case "topic" =>
        stages +=(tf,
          new LDA()
            .setFeaturesCol(RAW_FEATURES)
            .setTopicDistributionCol(FEATURES)
            .setK(Params.topicParamTopicNTopics)
            .setMaxIter(Params.topicParamTopicNIter)
          )
      case "w2v" =>
        stages +=
          new Word2Vec()
            .setInputCol(STOPWORD_BRAND_WORDS)
            .setOutputCol(FEATURES)
            .setVectorSize(Params.topicParamWord2vecSize)
            .setMaxIter(Params.topicParamWord2vecNIter)
            .setNumPartitions(Params.topicParamWord2vecNPartition)
            .setWindowSize(Params.topicParamWord2vecWindowSize)
            .setMinCount(Params.topicParamWord2vecMinCount)
      case _ => {
        throw new Exception(s"space not valid: ${Params.commonVecSpace}")
      }
    }

    // 构建模型
    val alg = Params.commonAlg match {
      case "rf" =>
        stages +=
          new RandomForestClassifier()
            .setLabelCol(INDEXED_LABEL)
            .setFeaturesCol(FEATURES)
            .setNumTrees(Params.rfTreesNum)
      case "lr" => {
        val c = new LogisticRegression()
          .setRegParam(Params.lrRegParam)
          .setElasticNetParam(Params.lrElasticNetParam)
          .setMaxIter(Params.lrNIter)
          .setLabelCol(INDEXED_LABEL)
          .setFeaturesCol(FEATURES)

        stages +=
          new OneVsRest()
            .setLabelCol(INDEXED_LABEL)
            .setFeaturesCol(FEATURES)
            .setClassifier(c)
      }
      case "nb" =>
        stages += new NaiveBayes()
          .setLabelCol(INDEXED_LABEL)
          .setFeaturesCol(FEATURES)
          .setModelType("multinomial")
      case _ => {
        throw new Exception(s"alg not valid: ${Params.commonAlg}")
      }
    }

    // debug 部分
    if (data != None) {
      println("training data debug information")

      val pl = Params.commonVecSpace match {
        case c: String if (c == "word" || c == "topic") =>
          new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans,
            remover, tf))
        case _ =>
          new Pipeline().setStages(Array(tokenizer, quantifier, medicine, sqlTrans,
            remover))
      }

      pl.fit(data.get).transform(data.get).show(100, false)
    }

    new Pipeline().setStages(stages toArray)
  }

  def main(args: Array[String]): Unit = {
    // 是不是 debug 模式
    val debug = args.length match {
      case len: Int if len > 0 => args(0) toBoolean
      case _ => false
    }

    println(s"debug is: $debug")

    // 加载配置
    println(s"params info: ${Params.toString}")

    if (!checkParams) {
      println("param is not ok.")
      System.exit(1)
    }

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
    val rawDataFrame = sqlContext.read.json(Params.trainPath)

    // 如果是 topic 模型, 需要对文本做一个 merge
    val rawTrainingDataFrame = Params.commonNMerge match {
      case x: Int if x > 1 => {
        val m: Int = Params.commonNMerge
        val partition: Int = Params.trainNPartition
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
    val trainingDataFrame = labelModel.transform(rawTrainingDataFrame).repartition(Params.trainNPartition)

    println("training data schema")
    trainingDataFrame.printSchema()

    // 加载测试数据
    val rawTestDataFrame = sqlContext.read.json(Params.testFilePath)
    val testDataFrame = labelModel.transform(rawTestDataFrame)

    println("test data schema")
    testDataFrame.printSchema()

    // 构造 pipeline, 训练模型
    val pipeline = constructPipeline(sqlContext, if (debug == true) Some(trainingDataFrame) else None)

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
    if (Params.testResultSave) {
      println("test result save")
      testResult.select(ID, BRAND, TITLE, CATEGORY, PREDICTED_LABEL).write.format("json").save(Params.testResultPath)
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