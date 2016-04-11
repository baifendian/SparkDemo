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

import org.apache.spark.{SparkConf, SparkContext}

case class Person(name: String, sex: String)

object ParseObject {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: [objectfile]")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("ParseObject")
    val ctx = new SparkContext(sparkConf)

    val objectFile = args(0)

    val data = ctx.parallelize(List(("qifeng.dai", "man"), ("tianwei.dai", "girl"), ("xinghe.dai", "boy"))).
      map { case (name, sex) => Person(name, sex) }.
      saveAsObjectFile(objectFile)

    ctx.objectFile(objectFile).collect().foreach(println)

    ctx.stop()
  }
}