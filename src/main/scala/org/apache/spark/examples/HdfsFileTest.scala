/**
 * Copyright (C) 2015 Baifendian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object HdfsFileTest {
  private def printUsage(): Unit = {
    val usage: String =
      "Usage: localFile dfsDir\n" +
      "\n" +
      "localFile - (string) local file to use in test\n" +
      "dfsDir - (string) DFS directory for read/write tests\n"

    println(usage)
  }

  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      printUsage()
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HdfsFileTest")
    val ctx = new SparkContext(sparkConf)

    val localFilePath = args(0)
    val dfsDirPath = args(1)
    val lineList = Source.fromFile(new File(localFilePath)).getLines().toList

    val localWordCount = runLocalWordCount(lineList)

    ctx.parallelize(lineList).saveAsTextFile(dfsDirPath)

    val dfsWordCount = ctx.textFile(dfsDirPath).
      flatMap(_.split("\\s+")).
      filter(_.nonEmpty).
      map(w => (w, 1)).
      countByKey().
      values.
      sum

    ctx.stop()

    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) agree.")
    } else {
      println(s"Failure! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) disagree.")
    }
  }
}