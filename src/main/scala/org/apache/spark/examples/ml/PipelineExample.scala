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
package org.apache.spark.examples.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object PipelineExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PipelineExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val training = sqlContext.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // 配置一个 ML 的 pipeline, 包括 3 个 stages，为: tokenizer, hashingTF, lr
    val tokenizer = new Tokenizer().
      setInputCol("text").
      setOutputCol("words")
    val hashingTF = new HashingTF().
      setNumFeatures(1000).
      setInputCol(tokenizer.getOutputCol).
      setOutputCol("features")
    val lr = new LogisticRegression().
      setMaxIter(10).
      setRegParam(0.01)
    val pipeline = new Pipeline().
      setStages(Array(tokenizer, hashingTF, lr))

    // 拟合 pipeline 和训练数据
    val model = pipeline.fit(training)

    // 保存 model 到 disk
    model.save("/tmp/spark-logistic-regression-model")

    // 同样可以保存未拟合数据到 disk
    pipeline.save("/tmp/unfit-lr-model")

    // 加载 model
    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    // 待测试数据，包括 (id, features) 的 tuple
    val test = sqlContext.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // 对测试数据进行预测
    sameModel.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }

    sc.stop()
  }
}