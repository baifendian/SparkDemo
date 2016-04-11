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

import sys, os
from pyspark import SparkConf, SparkContext
from pyspark.mllib.fpm import FPGrowth
reload(sys)
sys.setdefaultencoding('utf8')
dict = []
#将食物名字和id对应起来
def readFile():
    file = open("name.txt", 'r')
    for line in file:
        idx = 0
        items = line.split(' ')
        for item in items:
            if (len(item) >= 1):
                dict.append(item)
                idx = idx + 1

conf = SparkConf() \
      .setAppName(sys.argv[0])\
      .set("spark.executor.memory", "2g")\
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
sc = SparkContext(conf=conf)
readFile()
data = sc.textFile(sys.argv[1])
transactions = data.map(lambda line: line.strip().split(' '))
model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:
    for item in fi.items:
        for i in item:
            sys.stdout.write(dict[int(i)] + " ")
        print "\n"
