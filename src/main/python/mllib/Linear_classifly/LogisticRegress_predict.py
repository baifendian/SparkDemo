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
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint

def parsePoint(line):
    data = line[1:][:-1]
    values = [float(x) for x in data.split(', ')]
    return LabeledPoint(1 if values[34] > 0.5 else 0, values[:-1])

conf = SparkConf() \
      .setAppName(sys.argv[0])\
      .set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)
data = sc.textFile(sys.argv[1])
parsedData = data.map(parsePoint)

model = LogisticRegressionModel.load(sc, "Model_logistc")

labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Test Error = " + str(trainErr))
