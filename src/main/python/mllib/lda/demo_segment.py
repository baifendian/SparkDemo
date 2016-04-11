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
import jieba
from functools import partial

from pyspark import SparkContext, SparkConf

def func_seg(rdd_lines):
    res = []
    for line in rdd_lines:
        tag, text = line.strip().split("\t")
        words = list(jieba.cut(text))
	res.append(words)
    return res

def segment(rdd_lines): 
    '''
       分词
       分词对象占用内存对象比较大，每个partition只初始化一个
    '''
    parsed_data = rdd_lines.mapPartitions(func_seg)
    return parsed_data


def func_filter(rdd_lines, stopfile):
    res = []
    stopwords = []
    with open(stopfile, "r") as f:
        stopwords = [w.strip().decode("u8") for w in f.readlines()]
	stopwords.append(u" ")
        print "read %s stop words ..." % len(stopwords)

    for line in rdd_lines:
	words = [w for w in line if w not in stopwords]
	res.append(words)
    return res

def filter_stopword(rdd_words, stopfile):
    '''
       过滤停用词
       停用词典使用 --files 将文件分发到每个节点上进行读取
    '''
    parsed_data = rdd_words.mapPartitions(partial(func_filter, stopfile=stopfile))
    return parsed_data

if __name__ == '__main__': 
    if len(sys.argv) != 5:
        print >> sys.stderr, "Usage: %s <input> <output> <stopfile> name" % sys.argv[0]
        exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    stopfile = sys.argv[3]
    appname = sys.argv[4]

    # 初始化环境
    conf = SparkConf().setAppName(appname)
    sc = SparkContext(conf=conf)

    rdd_lines = sc.textFile(input_path)
    # 分词
    parsed_data = segment(rdd_lines).cache()

    # 过滤停用词
    if stopfile:
	parsed_data = filter_stopword(parsed_data, stopfile)            
 
    # 输出结果
    parsed_data = parsed_data.map(lambda x : u" ".join(x).encode("u8")) 
    parsed_data.saveAsTextFile(output_path)
    sc.stop()


