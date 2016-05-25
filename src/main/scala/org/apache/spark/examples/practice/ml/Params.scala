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

import org.apache.spark.examples.practice.utils.ConfigParser

import scala.collection.mutable.ArrayBuffer

object Params {
  // === 加载通用的配置信息 ===
  val commonModelPath = ConfigParser.getInstance.getString("common.model.path")
  val commonIndexModelPath = ConfigParser.getInstance.getString("common.indexmodel.path")
  val commonVecSpace = ConfigParser.getInstance.getString("common.vec_space")
  val commonNMerge = ConfigParser.getInstance.getInt("common.n_merge")
  val commonAlg = ConfigParser.getInstance.getString("common.alg")
  val commonTF = ConfigParser.getInstance.getString("common.tf")

  // === 加载 topic 主题空间需要的参数
  val topicParamTopicNTopics = ConfigParser.getInstance.getInt("topic_param.topic.n_topics")
  val topicParamTopicNIter = ConfigParser.getInstance.getInt("topic_param.topic.n_iter")

  val topicParamWord2vecSize = ConfigParser.getInstance.getInt("topic_param.word2vec.size")
  val topicParamWord2vecNIter = ConfigParser.getInstance.getInt("topic_param.word2vec.n_iter")
  val topicParamWord2vecNPartition = ConfigParser.getInstance.getInt("topic_param.word2vec.n_partition")
  val topicParamWord2vecWindowSize = ConfigParser.getInstance.getInt("topic_param.word2vec.window_size")
  val topicParamWord2vecMinCount = ConfigParser.getInstance.getInt("topic_param.word2vec.min_count")

  // === 加载训练需要的配置信息 ===
  val trainPath = ConfigParser.getInstance.getString("train.path")
  val trainNPartition = ConfigParser.getInstance.getInt("train.n_partition")

  // === tf-idf 的参数
  val tfidfFeaturesSelect = ConfigParser.getInstance.getInt("tfidf.features.select")
  val hashTFNumber = ConfigParser.getInstance.getInt("hash.tf_number")
  val countvecVocabSize = ConfigParser.getInstance.getInt("countvec.vocab.size")
  val countvecMinDF = ConfigParser.getInstance.getInt("countvec.mindf")

  // === 随机森林模型的参数
  val rfTreesNum = ConfigParser.getInstance.getInt("rf.trees.num")

  // === LR 的参数
  val lrNIter = ConfigParser.getInstance.getInt("lr.n_iter")
  val lrRegParam = ConfigParser.getInstance.getDouble("lr.regparam")
  val lrElasticNetParam = ConfigParser.getInstance.getDouble("lr.elastic_net_param")

  // === 加载预测需要的配置信息 ===
  val predictZookeeper = ConfigParser.getInstance.getString("predict.zookeeper")
  val predictGroupid = ConfigParser.getInstance.getString("predict.groupid")
  val predictTopics = ConfigParser.getInstance.getString("predict.topics")
  val predictNumStreams = ConfigParser.getInstance.getInt("predict.num.streams")

  val predictRedisHost = ConfigParser.getInstance.getString("predict.redis.host")
  val predictRedisPort = ConfigParser.getInstance.getInt("predict.redis.port")

  // === 文件预测的方式 ===
  val testFilePath = ConfigParser.getInstance.getString("test.path")
  val testResultSave = ConfigParser.getInstance.getBoolean("test.result.save")
  val testResultPath = ConfigParser.getInstance.getString("test.result.path")

  // === 加载自定义词典 ===
  val userDict = loadUserDict

  // === 加载预处理的词库 ===
  val preprocessMedicineFile = ConfigParser.getInstance.getString("preprocess.medicine_file")
  val preprocessQuantifierFile = ConfigParser.getInstance.getString("preprocess.quantifier_file")
  val preprocessStopFile = ConfigParser.getInstance.getString("preprocess.stop_file")

  private def loadUserDict: Array[String] = {
    val t_userDict = ArrayBuffer[String]()

    for (key <- ConfigParser.getInstance.getProperties.keySet().toArray()) {
      key match {
        case e: String => if (e.startsWith("segmenter.user_dict")) t_userDict += ConfigParser.getInstance.getString(e)
        case _ =>
      }
    }

    t_userDict toArray
  }

  // 对参数进行格式化处理返回
  override def toString(): String = {
    ConfigParser.getInstance.toString()
  }
}