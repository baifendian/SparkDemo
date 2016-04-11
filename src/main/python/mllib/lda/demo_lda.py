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
# limitations under the License.# -*- coding:utf-8 -*-

import sys, os, json
import jieba

from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vectors

from demo_segment import segment, filter_stopword
from demo_vectorize import vectorize

def lda(parsedData, topic_num):
    corpus = parsedData.zipWithIndex().map(lambda x: [x[1], x[0]]).cache()
    ldaModel = LDA.train(corpus, k=topic_num)
    return ldaModel


if __name__ == '__main__': 
    if len(sys.argv) != 6:
        print >> sys.stderr, "Usage: %s <input> <model_path> <stopfile> topic_num name" % sys.argv[0]
        exit(1)

    input_path = sys.argv[1]
    model_path = sys.argv[2]
    stopfile = sys.argv[3]
    topic_num = int(sys.argv[4])
    appname = sys.argv[5]

    conf = SparkConf().setAppName(appname)
    sc = SparkContext(conf=conf)

    rdd_lines = sc.textFile(input_path)
    parsed_data = segment(rdd_lines)

    if stopfile:
	parsed_data = filter_stopword(parsed_data, stopfile).cache()  

    tf_data = vectorize(sc, parsed_data)
    ldaModel = lda(tf_data, topic_num)
    
    print "finish train model..."
    t = ldaModel.describeTopics(5)
    print t

    ldaModel.save(sc, model_path)
    sc.stop()
