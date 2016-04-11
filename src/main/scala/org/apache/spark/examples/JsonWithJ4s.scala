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
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

// Note: 该 class 必须在 top-level
case class UserAction(cid: String, method: String, times: Int) {
  def this(cid: String, method: String) = {
    this(cid, method, 0)
  }
}

object JsonWithJ4s {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: [inputfile] [outputfile]")
      sys.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1)

    val sparkConf = new SparkConf().setAppName("JsonWithJ4s")
    val ctx = new SparkContext(sparkConf)

    val input = ctx.textFile(inputFile)

    // 这里需要注意的是：
    // 1. formats 需要通过 driver 分发到 task，但是没有继承序列化接口，所以可以是定义在 mapPartitions 中，每个 partition 中有一个
    // 实例，减少了开销
    // 2. flatMap 结合 Option 是比较有意思的，比如 val s = List(None, Some(1), Some(2))，s.flatMap(x => x) 的结果是 List(1,2)
    val useractions = input.mapPartitions(records => {
      implicit val formats = DefaultFormats // Brings in default date formats etc.

      records.flatMap(r => {
        try {
          Some(parse(r).extract[UserAction])
        } catch {
          case e: Exception => None
        }
      })
    }, true)

    // 以 cid, method 为 key，计算其出现的次数，最后以 json 的格式写入到了 hdfs 文件中
    useractions.
      map(x => (s"${x.cid}:${x.method}", 1)).
      reduceByKey(_ + _).
      mapPartitions(records => {
      // 方式一:
      //      implicit val formats = Serialization.formats(NoTypeHints)
      //      records.map({
      //        case (cidAndMethod, times) => write(UserAction(cidAndMethod.split(":")(0), cidAndMethod.split(":")(1), times))
      //      })

      // 方式二:
      records.map({
        case (cidAndMethod, times) => {
          val json: JObject = ("cid" -> cidAndMethod.split(":")(0)) ~ ("method" -> cidAndMethod.split(":")(0)) ~ ("times" -> times)
          compact(render(json))
        }
      })

    }).
      saveAsTextFile(outputFile)

    ctx.stop()
  }
}