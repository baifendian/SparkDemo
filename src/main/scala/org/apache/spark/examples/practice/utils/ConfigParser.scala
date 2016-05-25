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
package org.apache.spark.examples.practice.utils

import java.io.FileInputStream
import java.util.Properties

class ConfigParser(filename: String) {
  protected val config: Properties = loadConfig(filename)

  private def loadConfig(filename: String): Properties = {
    val c = new Properties()
    c.load(new FileInputStream(filename))
    c
  }

  def getProperties: Properties = config

  def getString(key: String, defaultValue: String = ""): String = {
    config.getProperty(key, defaultValue)
  }

  def getInt(key: String, defaultValue: Int = 0): Int = {
    config.getProperty(key, defaultValue toString) toInt
  }

  def getLong(key: String, defaultValue: Long = 0): Long = {
    config.getProperty(key, defaultValue toString) toLong
  }

  def getDouble(key: String, defaultValue: Double = 0.0): Double = {
    config.getProperty(key, defaultValue toString) toDouble
  }

  def getBoolean(key: String, defaultValue: Boolean = false): Boolean = {
    config.getProperty(key, defaultValue toString) toBoolean
  }

  // 对参数进行格式化处理返回
  override def toString(): String = {
    // 迭代输出配置中的信息
    val ss = for (key <- config.keySet().toArray()) yield {
      key match {
        case e: String => s"${e}=${config.getProperty(e)}"
        case _ =>
      }
    }

    ss.mkString(", ")
  }
}

object ConfigParser {
  private val DEFAULT_FILE = "props"

  @volatile private var instance: ConfigParser = null

  def getInstance: ConfigParser = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = new ConfigParser(DEFAULT_FILE)
        }
      }
    }

    instance
  }
}