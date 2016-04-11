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

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: kafka_wordcount.py <zk> <topic> <groupid> <output>")
        exit(-1)

    # spark.streaming.kafka.maxRatePerPartition to set the maximum number of messages per partition per batch
    conf = SparkConf().set("spark.default.parallelism", "2").set("spark.streaming.kafka.maxRatePerPartition", 1000)
    sc = SparkContext(appName="PythonStreamingKafkaWordCount", conf=conf)
    ssc = StreamingContext(sc, 20)

    zkQuorum, topic, groupid, output = sys.argv[1:]
    # 设置访问zk的便宜量，分largest 和smallest，默认处理是largest
    kafkaParams = {"auto.offset.reset": "smallest"}
    # Dict of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread
    kvs = KafkaUtils.createStream(ssc, zkQuorum, groupid, {topic: 2}, kafkaParams)
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    import datetime
    now = datetime.datetime.now()
    directory = now.strftime("%Y-%m-%d/%H-%M-%S")
    counts.saveAsTextFiles("%s/%s" %(output, directory), "txt")

    ssc.start()
    ssc.awaitTermination()
