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

import sys, os, json

from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF

from demo_segment import segment, filter_stopword

def vectorize(sc, rdd_words, size=0):
    '''
       使用TF将词语向量化
       向量的维度需要设定的，默认为2^20
    '''
    if not size:
    	size = rdd_words.flatMap(lambda x:x).distinct().count() + 10000
    hashingTF = HashingTF(size)
    tf = hashingTF.transform(rdd_words)
    return tf


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print >> sys.stderr, "Usage: %s <input> <output> <stopfile> name" % sys.argv[0]
        exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    stopfile = sys.argv[3]
    appname = sys.argv[4]

    conf = SparkConf().setAppName(appname)
    sc = SparkContext(conf=conf)

    rdd_lines = sc.textFile(input_path)
    parsed_data = segment(rdd_lines).cache()

    if stopfile:
	parsed_data = filter_stopword(parsed_data, stopfile).cache()            

    tf_data = vectorize(sc, parsed_data)
    tf_data.saveAsTextFile(output_path)
    sc.stop()


