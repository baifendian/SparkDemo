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
import redis
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def getWordlist(sparkContext):
    if ('wordlist' not in globals()):
        globals()['wordlist'] = sparkContext.broadcast(["wenting", "dongshen", "qifeng"])
    return globals()['wordlist']

def getRedisConnection():
    if("redisConnection" not in globals()):
        globals()['redisConnection'] = redis.ConnectionPool(host='172.18.1.22', port=6379, db=1)
    return globals()['redisConnection']


# Dstreams transform 数据转换成新的Dstreams(因为Dstreams 没有filter函数, 测试transform)
def process_1(rdd):
    wordlist = getWordlist(rdd.context)
    def fun(data):
        print data
        if data[0] in wordlist.value:
            return True
        else:
            return False
    return rdd.filter(fun)

# 打印测试
def process_2(time, rdd):
    counts = "Counts at time %s %s" % (time, rdd.collect())
    print(counts)

# 此种方式由于需要连接池序列化 会报错，rdd 的foreach 在work中执行， 需要将连接池序列化从driver发送到worker
def process_3(time, rdd):
    # This is incorrect as this requires the connection object to be serialized and sent from the driver to the worker
    pool = getRedisConnection()
    r = redis.Redis(connection_pool=pool)
    rdd.foreach(lambda record: r.set(record[0], record[1]))

# 一条记录获取一个连接
def process_4(rdd):
    # one record get one pool
    pool = getRedisConnection()
    r = redis.Redis(connection_pool=pool)
    r.set(rdd[0], rdd[1])

# 一个批处理或者一个partition 获取一个连接
def process(iter):
    pool = getRedisConnection()
    r = redis.Redis(connection_pool=pool)
    for record in iter:
        r.set(record[0], record[1])


def createContext(host, port, checkpoing):
    print("Create new context")
    sc = SparkContext(appName="PythonStreamComplex")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint(checkpoint)
    lines = ssc.socketTextStream(host, port)
    words = lines.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda x: (x, 1)).reduceByKeyAndWindow(lambda x, y: x + y, 600, 300)
    filterCounts = wordCounts.transform(process_1)
    filterCounts.foreachRDD(process_2)
    #filterCounts.foreachRDD(lambda rdd: rdd.foreach(process_4))
    filterCounts.foreachRDD(lambda rdd: rdd.foreachPartition(process))
    return ssc


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: <hostanme> <port> <checkpoint-directory>")
        exit(-1)
    host, port, checkpoint = sys.argv[1:]
    ssc = StreamingContext.getOrCreate(checkpoint,
                                       lambda: createContext(host, int(port), checkpoint))

    ssc.start()
    ssc.awaitTermination()
