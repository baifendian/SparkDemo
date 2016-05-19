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

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.collection.mutable.Set

class QuantifierProcess(override val uid: String, private val dict: String)
  extends UnaryTransformer[Seq[String], Seq[String], QuantifierProcess] {

  def this(dict: String) = this(Identifiable.randomUID("quan"), dict)

  // 词典文件的正则形式
  private val wordsSet = loadDict
  private val numRegex = """^\d+\.?\d*|[%s]+""".r

  // 词典加载
  private def loadDict: Set[String] = {
    val br: BufferedReader = Files.newBufferedReader(Paths.get(dict), StandardCharsets.UTF_8)
    val words = Set[String]()

    var count = 0

    while (br.ready()) {
      words += br.readLine()
      count += 1
    }

    println(s"load quantifier words: $count")

    words
  }

  override protected def createTransformFunc: Seq[String] => Seq[String] = (words: Seq[String]) => {
    // 处理 "单位词", arr 是前面处理过的单元, c 是当前要处理的 word
    words.foldLeft(List[String]())((arr, c) => {
      val p = arr.lastOption
      var newC = c

      // 是不是要参考前一个单词 p
      val useP = p match {
        case None => false
        case Some(e) => if (numRegex.replaceAllIn(e, "") == "") true else false
      }

      val isQuan =
        useP match {
          case true =>
            // p 是数字
            if (wordsSet.contains(c)) true
            else false
          case false => {
            val tmp = numRegex.replaceAllIn(c, "")
            // p 不是数字或不存在
            if (wordsSet.contains(tmp)) {
              newC = tmp
              true
            }
            else {
              false
            }
          }
        }

      // 返回 List, 这里对数字进行了过滤
      if (numRegex.replaceAllIn(c, "") == "") arr
      else
        arr :+ {
          isQuan match {
            case true => s"_QUAN_$newC"
            case false => newC
          }
        }
    })
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): QuantifierProcess = defaultCopy(extra)
}