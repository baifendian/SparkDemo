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

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ValidationSplitExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <regression_data>")
      System.exit(1)
    }

    val tData = args(0)

    val conf = new SparkConf().setAppName("ValidationSplitExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 准备训练和测试数据
    val data = sqlContext.read.format("libsvm").load(tData)

    // 训练和测试集合的切分构造
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LinearRegression()

    // 使用 ParamGridBuilder 构造参数网格，TrainValidationSplit 会对所有的 values 组合进行测试，使用 evaluator 选择最佳模型
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // estimator 是 linear regression 模型，TrainValidationSplit 需要一个 Estimator, 以及 Estimator ParamMaps 和 Evaluator
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% 的数据用于 training，20% 的数据用于 test
      .setTrainRatio(0.8)

    // 运行 train 的 split validation, 选择最好的模型参数
    val model = trainValidationSplit.fit(training)

    // 在测试集合上做预测，model 是我们选择的最佳模型
    model.transform(test)
      .select("features", "label", "prediction")
      .show()

    sc.stop()
  }
}