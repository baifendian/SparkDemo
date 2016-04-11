# -*- coding: utf-8 -*-

# Copyright (C) 2015 Baifendian Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def func(rdd):
     datas = rdd.collect()
     for data in datas:
        print(data)

# 通过nc -lk 9999 来进行发送数据
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>")
        exit(-1)
    # 创建sparkContext
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    # batchDuration = 10 秒 每隔10秒生成一个job
    ssc = StreamingContext(sc, 10)

    # 创建Dstreams，读取对应ip 和 端口的数据
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    # wordcounts 统计
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b, 4)
    
    # 输出数据，可以输出到对应的外围系统，数据库等，此函数在driver进程中运行
    # 此函数中经常会执行action操作，来强制streaming RDD 的执行
    counts.foreachRDD(func)
    ssc.start()
    ssc.awaitTermination()
