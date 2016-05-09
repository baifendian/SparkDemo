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
package org.apache.spark.examples.ml.regression

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{DecisionTreeRegressor, DecisionTreeRegressionModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object DecisionTreeRegressionExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <regression_data>")
      System.exit(1)
    }

    val inputData = args(0)

    val conf = new SparkConf().setAppName("DecisionTreeRegressionExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 加载训练数据
    val data = sqlContext.read.format("libsvm").load(inputData)

    // 自动的识别 categorical features, 对其进行 index， 对于具有不同值 > 4 的特征认为是连续的特征
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // 对数据进行切分，分为训练和测试集合(30% 用于测试)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 训练决策树模型: DecisionTree model
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // 构建 Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, dt))

    // 训练模型，这也会运行 indexer
    val model = pipeline.fit(trainingData)

    // 进行相应的预测
    val predictions = model.transform(testData)

    // 选择需要展示的信息
    predictions.select("prediction", "label", "features").show(5)

    // 选择 (prediction, true label)，计算测试误差
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModel.toDebugString)

    sc.stop()
  }
}