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
package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/*
为了测试 spark 内部原理写的一个程序，读者可以忽略之。

hadoop: 2.6.0
spark: 1.6.0
集群 cores: 32
每个 node 的 cores 限制: 8

spark-submit --class org.apache.spark.examples.ExceptionTest --master yarn --deploy-mode client --driver-cores 1 --driver-memory 512M --num-executors 1 --executor-cores 2 --executor-memory 512M spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar xx yy

我们想观察以下几种情况下 hadoop ui 和 spark ui 的异同:
- driver 分配资源不合理/内存溢出(提交的时候，故意分配不合理的 core 或者 memory 数目, 对于 cluster/client 模式都要测试)
  driver core is 0:
    hadoop ui: FINISHED	SUCCEEDED

  driver core is 31:
    hadoop ui: FINISHED	SUCCEEDED

  driver core is 33:
    hadoop ui: FINISHED	SUCCEEDED

  driver memory is 512m, 参数为 200m:
    hadoop ui: FINISHED	SUCCEEDED
    实际上这里我们的 executor 已经分配了，但是看 spark ui 发现任务没有执行，没有提交任何的 stage

- executor 分配资源不合理/内存溢出(提交的时候，故意分配不合理的 core 或者 memory 数目)
    executors is 0:
      任务提交失败，java.lang.AssertionError

    executor core is 0:
      任务提交失败，java.lang.AssertionError

    executor core is 31:
      hadoop ui: FINISHED	SUCCEEDED
      注意这里如果是多个 executor 也是可以的

    executor core is 33:
      hadoop ui: FINISHED	FAILED
      任务出现异常，java.lang.NullPointerException

    executor memory is 512m, 参数为 200m:
      hadoop ui: ACCEPTED	UNDEFINED
      看不到 spark ui 日志，能看到结点上的异常日志。
      16/04/27 11:26:07 WARN YarnAllocator: Container marked as failed: container_1456208267763_6997_01_000002 on host: bgsbtsp0007-dqf. Exit status: 143. Diagnostics: Container killed on request. Exit code is 143
      Container exited with a non-zero exit code 143
      Killed by external signal

- 程序异常退出，driver 被 kill(我们把 driver kill 掉，client 模式)
  直接 kill driver 进程
  hadoop ui: ACCEPTED	UNDEFINED

- 程序被 application kill(我们把 application kill 掉，通过 yarn 的命令)
  command: yarn application -kill xxx
  hadoop ui: KILLED	KILLED

- 程序在初始化后 throws exception
  hadoop ui: FINISHED	SUCCEEDED
 */
object ExceptionTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: throw-exception, driver-mb, executor-mb")
      System.exit(1)
    }

    val throwException = args(0) toBoolean
    val driverMb = args(1) toInt
    val executorMb = args(2) toInt

    val sparkConf = new SparkConf().setAppName("ExceptionTest")
    val sc = new SparkContext(sparkConf)

    if (throwException) {
      throw new Exception("throw exception.")
    } else {
      // 让 driver 内存溢出
      val data = new Array[String](driverMb * 1024 * 1024)

      val v = sc.parallelize(1 to 10000, 100).map(x => {
        Thread.sleep(50)
        // 让 executor 内存溢出
        val data = new Array[String](executorMb * 1024 * 1024)

        x + 1
      }).reduce(_ + _)

      println(v)

      sc.stop()
    }
  }
}