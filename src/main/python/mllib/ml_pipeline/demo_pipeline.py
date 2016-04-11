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

import os, sys, json

import jieba
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import Row
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Word2Vec
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StopWordsRemover

def text_to_df(sc, sqlContext, input):
    '''
	读取文本，然后分词，并转化为DataFrame格式
    '''
    lines = sc.textFile(input)
    #parts = lines.map(lambda l : l.split("\t"))
    print "load text OK ..."
    parts = lines.mapPartitions(func_segment) 
    corpus = parts.map(lambda p: Row(label=p[0], text=p[1]))
    print "segment OK ..."
    data_df = sqlContext.createDataFrame(corpus)
    return data_df

def func_segment(rdd_lines):
    res = []
    for line in rdd_lines:
        tag, text = line.strip().split("\t")
        words = list(jieba.cut(text))
	res.append([tag, ' '.join(words)])
    return res
    
def get_pipeline(vector_size=50, class_num=5, stopwords=None):
    '''
	构建pipeline
        该demo pipeline包含以下步骤：
	1. labelIndexer 将标签索引，从字符装化为整数
        2. tokenizer 将句子分成单词
        3. remover 去除停用词
        4. word2vec 使用word2vec将文本转化为低维度向量
        5. mpc 神经网络分类器
    ''' 
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexLabel")
    tokenizer = Tokenizer(inputCol="text", outputCol="raw_words")
    remover = StopWordsRemover(inputCol="raw_words", outputCol="words", stopWords=stopwords)
    word2vec = Word2Vec(vectorSize=vector_size, minCount=2, inputCol="words", outputCol="vector")
    layers = [vector_size, (vector_size+class_num)/2, class_num]
    mpc = MultilayerPerceptronClassifier(maxIter=100, layers=layers, seed=1234, featuresCol="vector", labelCol="indexLabel") 
    pipeline = Pipeline(stages=[labelIndexer, tokenizer, remover, word2vec, mpc])
    return pipeline
    
def get_stopwords(stopfile):
    '''
 	获取停用词
    '''
    sfile = open(stopfile, "r")
    stopwords = sfile.readlines()
    stopwords = [w.strip().decode("u8") for w in stopwords]
    sfile.close()
    stopwords.append(u" ")
    stopwords.append(u"　")
    return stopwords

if __name__ == "__main__":
    if len(sys.argv) != 6:
       	print >> sys.stderr, "%s <input> <model_path> <stop_file> class_num appname" % sys.argv[0] 
        sys.exit(1)

    input_path = sys.argv[1]
    model_path = sys.argv[2]
    stop_file = sys.argv[3]
    class_num = int(sys.argv[4])
    appname = sys.argv[5]

    conf = SparkConf().setAppName(appname)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    data_df = text_to_df(sc, sqlContext, input_path)
    print "*** create data frame ***" 
    splits = data_df.randomSplit([0.8, 0.2], 1234)
    training = splits[0].cache()
    test = splits[1].cache()

    stopwords = get_stopwords(stop_file)
    print "*** load %s stopwords ***" % len(stopwords)
    pipeline = get_pipeline(vector_size=50, class_num=class_num, stopwords=stopwords) 
    model = pipeline.fit(training)
    result = model.transform(test)
  
    pred_label = result.select("prediction", "indexLabel")
    evaluator = MulticlassClassificationEvaluator(metricName="precision", predictionCol="prediction", labelCol="indexLabel")
    print("Precision: " + str(evaluator.evaluate(pred_label)))    


