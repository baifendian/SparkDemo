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
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: local file, local or hdfs <file>")
        exit(-1)
    conf = SparkConf().set("spark.default.parallelism", "3")

    sc = SparkContext(appName="TextFile", conf=conf)
    # 如果是读本地文件 需要设置加file://(path 全路径)
    if sys.argv[2] == "local":
        path = "file://%s" % sys.argv[1]
    else:
        path = sys.argv[1]

    # 读取textFile 文件
    lines = sc.textFile(path , 2, use_unicode=False)

    # hadoop 老的api， mapred
    #lines = sc.hadoopFile(path, "org.apache.hadoop.mapred.TextInputFormat",
    #                            "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")

    # hadoop 新的api  mapreduce
    #lines = sc.newAPIHadoopFile(path, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
    #                            "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")

    # hadoop 新api 中getSplits getSplits(JobContext context)
    # hadoop  旧的api 中 getSplits(JobConf job, int numSplits)
    #  textFile 用的旧的api 和 hadoopFile 相同 打印2个partition
    #  newApiHadoopFile 打印一个partition 
    print "textfile partitions: %s" % lines.getNumPartitions()
    print lines.count()

    sc.stop()
