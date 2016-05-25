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

import java.io.FileInputStream
import java.util.Properties

import scala.collection.mutable.ArrayBuffer

class ConfigParser(filename: String) {
  def this() = {
    this("props")
  }

  private val config: Properties = new Properties()

  config.load(new FileInputStream(filename))

  // === 加载通用的配置信息 ===
  val commonModelPath: String = config.getProperty("common.model.path")
  val commonIndexModelPath: String = config.getProperty("common.indexmodel.path")
  val commonVecSpace: String = config.getProperty("common.vec_space")
  val commonTFNumber: Int = config.getProperty("common.tf_number") toInt
  val commonNMerge: Int = config.getProperty("common.n_merge") toInt
  val commonAlg = config.getProperty("common.alg")

  // === 加载 topic 主题空间需要的参数
  val topicParamTopicNTopics: Int = config.getProperty("topic_param.topic.n_topics") toInt
  val topicParamTopicNIter: Int = config.getProperty("topic_param.topic.n_iter") toInt

  val topicParamWord2vecSize: Int = config.getProperty("topic_param.word2vec.size") toInt
  val topicParamWord2vecNIter: Int = config.getProperty("topic_param.word2vec.n_iter") toInt
  val topicParamWord2vecNPartition: Int = config.getProperty("topic_param.word2vec.n_partition") toInt
  val topicParamWord2vecWindowSize: Int = config.getProperty("topic_param.word2vec.window_size") toInt
  val topicParamWord2vecMinCount: Int = config.getProperty("topic_param.word2vec.min_count") toInt

  // === 加载训练需要的配置信息 ===
  val trainPath: String = config.getProperty("train.path")
  val trainNPartition: Int = config.getProperty("train.n_partition") toInt

  // === tf-idf 的参数
  val tfidfFeaturesSelect: Int = config.getProperty("tfidf.features.select") toInt

  // === 随机森林模型的参数
  val rfTreesNum: Int = config.getProperty("rf.trees.num") toInt

  // === LR 的参数
  val lrNIter = config.getProperty("lr.n_iter") toInt
  val lrRegParam = config.getProperty("lr.regparam") toDouble
  val lrElasticNetParam = config.getProperty("lr.elastic_net_param") toDouble

  // === 加载预测需要的配置信息 ===
  val predictZookeeper: String = config.getProperty("predict.zookeeper")
  val predictGroupid: String = config.getProperty("predict.groupid")
  val predictTopics: String = config.getProperty("predict.topics")
  val predictNumStreams: Int = config.getProperty("predict.num.streams") toInt

  val predictRedisHost = config.getProperty("predict.redis.host")
  val predictRedisPort = config.getProperty("predict.redis.port") toInt

  // === 文件预测的方式 ===
  val testFilePath = config.getProperty("test.path")
  val testResultSave = config.getProperty("test.result.save") toBoolean
  val testResultPath = config.getProperty("test.result.path")

  // === 加载自定义词典 ===
  val userDict = loadUserDict

  private def loadUserDict: Array[String] = {
    val t_userDict = ArrayBuffer[String]()

    for (key <- config.keySet().toArray()) {
      key match {
        case e: String => if (e.startsWith("segmenter.user_dict")) t_userDict += config.getProperty(e)
        case _ =>
      }
    }

    t_userDict toArray
  }

  // === 加载预处理的词库 ===
  val preprocessMedicineFile = config.getProperty("preprocess.medicine_file")
  val preprocessQuantifierFile = config.getProperty("preprocess.quantifier_file")
  val preprocessStopFile = config.getProperty("preprocess.stop_file")

  // 对参数进行格式化处理返回
  override def toString(): String = {
    s"commonModelPath: $commonModelPath " +
      s"commonIndexModelPath: $commonIndexModelPath " +
      s"commonVecSpace: $commonVecSpace " +
      s"commonTFNumber: $commonTFNumber " +
      s"commonNMerge: $commonNMerge " +
      s"commonAlg: $commonAlg " +
      s"topicParamTopicNTopics: $topicParamTopicNTopics " +
      s"topicParamTopicNIter: $topicParamTopicNIter " +
      s"topicParamWord2vecSize: $topicParamWord2vecSize " +
      s"topicParamWord2vecNIter: $topicParamWord2vecNIter " +
      s"topicParamWord2vecNPartition: $topicParamWord2vecNPartition " +
      s"topicParamWord2vecWindowSize: $topicParamWord2vecWindowSize " +
      s"topicParamWord2vecMinCount: $topicParamWord2vecMinCount " +
      s"trainPath: $trainPath " +
      s"trainNPartition: $trainNPartition " +
      s"tfidfFeaturesSelect: $tfidfFeaturesSelect " +
      s"rfTreesNum: $rfTreesNum " +
      s"lrNIter: $lrNIter " +
      s"lrRegParam: $lrRegParam " +
      s"lrElasticNetParam: $lrElasticNetParam " +
      s"predictZookeeper: $predictZookeeper " +
      s"predictGroupid: $predictGroupid " +
      s"predictTopics: $predictTopics " +
      s"predictNumStreams: $predictNumStreams " +
      s"predictRedisHost: $predictRedisHost " +
      s"predictRedisPort: $predictRedisPort " +
      s"testFilePath: $testFilePath " +
      s"testResultSave: $testResultSave " +
      s"testResultPath: $testResultPath " +
      s"""userDict: ${userDict.mkString(",")} """ +
      s"preprocessMedicineile: $preprocessMedicineFile " +
      s"preprocessQuantifierFile: $preprocessQuantifierFile " +
      s"preprocessStopFile: $preprocessStopFile"
  }
}

object ConfigParser {
  def apply(): ConfigParser = {
    new ConfigParser()
  }

  def apply(file: String): ConfigParser = {
    new ConfigParser(file)
  }
}