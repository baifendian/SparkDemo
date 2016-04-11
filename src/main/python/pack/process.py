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
import config
from data_process import DataProcess

from pyspark import SparkContext, SparkConf

# 每个partition 初始化一次，调用一次函数。
def process_partition(iterator):
    # 通过配置文件初始化DataProcess， DataProcess 主要是初始化hbase client
    conf = config.Config("process.conf")
    dp = DataProcess(conf)
    for line in iterator:
        dp.get_default("aaaaaaaa")
        result_lines = line.split(' ')
        for item in result_lines:
            yield item

def filter_wenting(element):
    if "wenting" in element:
        return True
    return False

def filter_dongshen(element):
    if "dongshen" in element:
        return True
    return False

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: process <file>")
        exit(-1)
    conf = SparkConf().set("spark.default.parallelism", "3")
    sc = SparkContext(appName="process_zip", conf=conf)

    # 读取数据分四个partition
    lines = sc.textFile(sys.argv[1], 4, use_unicode=False)
    
    # 每个partition 调用process_partition 并将结果cache 到内存中。
    # cache 到内存中的作用是，后面多个action 不用重新计算，访问thrift hbase 很耗资源。
    data = lines.mapPartitions(process_partition).cache()

    print data.count()
    data_wenting = data.filter(filter_wenting).count()
    print data_wenting
    data_dongshen = data.filter(filter_dongshen).count()
    print data_dongshen
    sc.stop()
