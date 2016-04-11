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
from pyspark.sql import SQLContext, Row


# 创建SQLContext 单例
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sql_network_wordcount.py <hostname> <port> ")
        exit(-1)
    host, port = sys.argv[1:]
    sc = SparkContext(appName="PythonSqlNetworkWordCount")
    ssc = StreamingContext(sc, 10)

    lines = ssc.socketTextStream(host, int(port))
    words = lines.flatMap(lambda line: line.split(" "))

    def process(time, rdd):
        print("========= %s =========" % str(time))
        try:
            sqlContext = getSqlContextInstance(rdd.context)
            # RDD[String] 转化成 RDD[Row]
            rowRdd = rdd.map(lambda w: Row(word=w))
            wordsDataFrame = sqlContext.createDataFrame(rowRdd)

            wordsDataFrame.registerTempTable("words")

            wordCountsDataFrame = \
                sqlContext.sql("select word, count(*) as total from words group by word")
            wordCountsDataFrame.show()
        except:
            pass
    #此函数在driver进程中运行 
    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
