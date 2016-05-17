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

import org.apache.log4j.Logger

object TextCategory {
  val logger = Logger.getLogger(getClass.getName)

  /**
    * 训练模型, 训练之后保存到指定的目录
    */
  def train: Unit = {
    // 加载配置
    val parser: ConfigParser = ConfigParser()

    // 对文件进行分词处理

    // 对文件进行预处理

    // 构建 Pipeline

    // 训练模型

  }

  /**
    * 预测过程, 从 kafka 读取数据进行预测, 预测的结果写到 redis 中
    */
  def predict: Unit = {

  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: [train/predict]")
      System.exit(1)
    }

    args(0) match {
      case "train" => train
      case "predict" => predict
      case _ => logger.error("in-valid command, must be train or predict.")
    }
  }
}