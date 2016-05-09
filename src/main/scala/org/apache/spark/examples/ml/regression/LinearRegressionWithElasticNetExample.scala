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

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegressionWithElasticNetExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <regression_data>")
      System.exit(1)
    }

    val inputData = args(0)

    val conf = new SparkConf().setAppName("LinearRegressionWithElasticNetExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 加载训练数据
    val training = sqlContext.read.format("libsvm").load(inputData)

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // 拟合模型
    val lrModel = lr.fit(training)

    // 打印相关系数和 linear regression 相关的解释
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // 对训练数据上的模型进行 Summarize，打印一些 metrics
    val trainingSummary = lrModel.summary

    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    sc.stop()
  }
}