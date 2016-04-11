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

import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import org.apache.spark.{SparkConf, SparkContext}

object ParseCsv {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: [inputfile] [outputfile]")
      sys.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1)

    val sparkConf = new SparkConf().setAppName("ParseCsv")
    val ctx = new SparkContext(sparkConf)

    val input = ctx.textFile(inputFile)

    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }

    // 这里是以 "," 分隔的
    val useractions = result.map(x => (s"${x(0)},${x(1)}", 1)).
      reduceByKey(_ + _).
      mapPartitions(records => {
        val stringWriter = new StringWriter()
        val csvWriter = new CSVWriter(stringWriter)
        records.foreach(v => csvWriter.writeNext(Array(s"${v._1},${v._2}")))
        Iterator(stringWriter.toString)
      }).
      saveAsTextFile(outputFile)

    ctx.stop()
  }
}