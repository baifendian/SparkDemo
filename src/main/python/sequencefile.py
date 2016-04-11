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
        print("Usage: input sequencefile,  output<file>")
        exit(-1)
    conf = SparkConf().set("spark.default.parallelism", "3")

    sc = SparkContext(appName="SequenceFile", conf=conf)
    path = sys.argv[1]
    out = sys.argv[2]

    # 读取sequence file 文件
    #lines = sc.sequenceFile(path, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.LongWritable")

    # hadoop 老的api， mapred
    #lines = sc.hadoopFile(path, "org.apache.hadoop.mapred.SequenceFileInputFormat",
    #                            "org.apache.hadoop.io.Text", "org.apache.hadoop.io.LongWritable")

    # hadoop 新的api  mapreduce
    lines = sc.newAPIHadoopFile(path, "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
                                "org.apache.hadoop.io.Text", "org.apache.hadoop.io.IntWritable")

    # hadoop 新api 中getSplits getSplits(JobContext context)
    # hadoop  旧的api 中 getSplits(JobConf job, int numSplits)
    #  textFile 用的旧的api 和 hadoopFile 相同 打印2个partition
    #  newApiHadoopFile 打印一个partition 
    print "sequence partitions: %s" % lines.getNumPartitions()

    results = lines.mapValues(lambda x: long(x))
    for i in results.take(10):
        print i[0] , i[1]
    print results.count()

    # saveAsSequenceFile(path, compressionCodecClass=None)
    lines.saveAsSequenceFile(out, "org.apache.hadoop.io.compress.GzipCodec")

    # 输入是IntWritable 输出是LongWritable ， 需要将value 进行long(value) 否则转换错误
    #results.saveAsNewAPIHadoopFile("/user/baseline/output/sequence", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
    #                             "org.apache.hadoop.io.Text", "org.apache.hadoop.io.LongWritable")

    sc.stop()

