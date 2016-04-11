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
from pyspark.streaming.kafka import KafkaUtils

offsetRanges = []

# 获取每个partition 的偏移量信息
def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

def createContext(brokers, topic):
    print("Create new context")
    conf = SparkConf().set("spark.default.parallelism", "2").set("spark.streaming.kafka.maxRatePerPartition", 1000)
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount", conf=conf)
    ssc = StreamingContext(sc, 10)
    # 直接 连接kafka，没有Receiver， 可以完全实现exactly-once
    # 具体exactly-once 可参考 http://blog.cloudera.com/blog/2015/03/exactly-once-spark-streaming-from-apache-kafka/
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"})
    kvs.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    return ssc

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic> <checkpoint>")
        exit(-1)

