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

import sys, json
import numpy, scipy
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.ml.feature import PCA
from pyspark.mllib.linalg import Vectors

#读取数据
def readFile(sc, sqlContext , file_path):
    data = sc.textFile(file_path)
    corpus = data.map(lambda x: json.loads(x)).\
             map(lambda x: Row(features=Vectors.dense(x[:-1]), label=x[-1]))
    return sqlContext.createDataFrame(corpus)

conf = SparkConf().setAppName(sys.argv[0])
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
data = readFile(sc, sqlContext, sys.argv[1])
pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
lr = LogisticRegression(maxIter=10, regParam=0.01, featuresCol='pcaFeatures',labelCol='label')
pipeline = Pipeline(stages=[pca, lr])
model = pipeline.fit(data)

#构造测试数据
test = sqlContext.createDataFrame([(1.0, Vectors.dense([30.0, 77516.0, 13.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2174.0, 0.0, 40.0]))], ["label", "features"])



prediction = model.transform(test)
selected = prediction.select("label", "features", "prediction")
for row in selected.collect():
    print(row)
