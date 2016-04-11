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
from operator import add

from pyspark import SparkConf, SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>")
        exit(-1)
    #设置配置参数
    conf = SparkConf().set("spark.default.parallelism", "3")
    sc = SparkContext(appName="PythonWordCount", conf=conf)
    # 函数 textFile(name, minPartitions=None, use_unicode=True)
    # use_unicode 设置成false，编码采用utf-8， 比unicode 小而且速度快。
    # 如果hdfs文件多个文件块时，如果块的个数大于设置的个数，采用文件块个数
    lines = sc.textFile(sys.argv[1], 2, use_unicode=False)
    print "textfile partitions: %s" % lines.getNumPartitions()

    # flatMap(f, preservesPartitioning=False)  
    # 函数f需要返回列表   x="a b" x.split(' ') 返回[a, b]
    #                     x ="a c d" x.split(' ') 返回[a, c, d]
    #                     flatMap  返回 [a, a, b, c, d]
    #                     map  后返回   [(a,1), (a,1), (b,1), (c,1), (d,1)]

    # reduceByKey(func, numPartitions=None, partitionFunc=<function portable_hash at 0x7fa664f3cb90>)
    # reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce
    # redeceByKey 会将本地的mapper进行合并然后发给reduce， 和MapReduce中的combiner类似
    # 输出的partitions 由numPartitions决定。如果conf设置了spark.default.parallelism 且rdd没有partition，此时用配置的值。
    # 如果没有设置spark.default.parallelism 用最大的upstream RDD的partition 个数，此时可能引起out-of-memory errors
    # partitionFunc 默认是hash-partition
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)

    print "shuffer partitions:%s " %lines.getNumPartitions()

    # driver 获取数据， 如果数据量太大， 不建议用collect，可以用first 等等别的操作
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    sc.stop()
