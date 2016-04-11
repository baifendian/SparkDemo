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

"""
创建表和对应的family
hbase(main):016:0> create 'test', 'f1'
> hbase_outputformat <host> test row1 f1 q1 value1
> hbase_outputformat <host> test row2 f1 q1 value2
> hbase_outputformat <host> test row3 f1 q1 value3
> hbase_outputformat <host> test row4 f1 q1 value4
"""
if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: hbase_outputformat <host> <table> <row> <family> <qualifier> <value>")
        exit(-1)

    host = sys.argv[1]
    table = sys.argv[2]
    sc = SparkContext(appName="HBaseOutputFormat")

    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    # 将String 转化成 ImmutableBytesWritable (详细见实例中scala python convert 代码)
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"

    # 将StringList 转化成 Put 结构
    # val put = new Put(output(0))
    # put.add(output(1), output(2), output(3))
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    sc.parallelize([sys.argv[3:]]).map(lambda x: (x[0], x)).saveAsNewAPIHadoopDataset(
        conf=conf,
        keyConverter=keyConv,
        valueConverter=valueConv)

    sc.stop()
