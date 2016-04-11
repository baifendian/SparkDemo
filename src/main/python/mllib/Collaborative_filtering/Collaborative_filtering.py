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
import time
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
#这个数据集中包含了71567位用户对10681部电影的总共10000054条评分数据。在movies.bat
#中是的数据格式是：movies_id::movies_name::other_imformation, 在ratings.bat中的格式
#是：user_id::movies_id::rating::time

#rating.bat文件处理，输出((哈希值，取time的最后一位数字),(user_id, mocies_id, rating))
def parseRating(line):
    allItem = line.split('::')
    return (int(allItem[3]) % 10, (allItem[0], allItem[1], allItem[2]))

#movies.bat文件处理，输出(movies_id, movies_name)
def parseMovie(line):
    fields = line.split("::")
    return (int(fields[0]), fields[1])

#计算model在data数据集上的均方误差(Mean Squared Error)
def computeRmse(model, data):
     newData = data.map(lambda r: (r[0], r[1]))
     predictions = model.predictAll(newData).map(lambda r : ((r[0], r[1]), r[2]))
     ratesAndPreds = data.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
     return ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()

#训练模型，注意，为了更好的调整参数，每个参数都使用了两个值最为备选值，通过
#使模型在用于调参的数据上的误差最小选取参数，这个可以参数表可以自己设置。
#train的参数有lambda_是正则化项，blocks表示分区数，设置为-1为使用默认配置
#iterations是迭代次数，rank是每一个user或movies的隐含因素的维数。注意，
#rank过大或lambda过小都可能导致过拟合，可能导致预测结果偏小
def train(training, validation, test, iterations=5, lambda_=0.01, blocks=-1):
    ranks = [8, 12]
    lambdas = [1.0, 10.0]
    numIters = [10, 20]
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1

    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
        model = ALS.train(training, rank, numIter, lmbda)
        validationRmse = computeRmse(model, validation)
        print "RMSE (validation) = %f for the model trained with " % validationRmse + \
              "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter)
        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter

    testRmse = computeRmse(bestModel, test)

    print "The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
          + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse) 
    return bestModel 

#预测。注意使用ALS算法时预测的user_id和movies都必须在训练集中。
def predict(model, rating, user_id):
    myRateMovieIdsRDD = rating.filter(lambda x: int(x[0]) == user_id).\
                map(lambda x: x[1]).collect()
    myRateMovieIds = set(myRateMovieIdsRDD)
    candidates = sc.parallelize([m for m in movies if m not in myRateMovieIds])
    predictions = model.predictAll(candidates.map(lambda x: (user_id, x))).collect()
    recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]
    print "Movies recommended for you:"
    for i in xrange(len(recommendations)):
        print ("%2d: %s" % (i + 1, movies[recommendations[i][1]])).encode('ascii', 'ignore')

if __name__ == "__main__":
    if (len(sys.argv) < 2):
        print "Usage: [usb root directory]/spark/bin/spark-submit --driver-memory\
               2g MovieLensALS.py movieLensDataDir personalRatingsFile"
        sys.exit(1)

    # 配置环境
    conf = SparkConf() \
      .setAppName(sys.argv[0])\
      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    # 导入评论数据
    ratingRDD = sc.textFile(sys.argv[1]).map(parseRating)
    
    # 导入电影数据
    movies = dict(sc.textFile(sys.argv[2]).map(parseMovie).collect())
    
    print "%d" % (ratingRDD.count())
    
    #将所有数据根据之前的哈希值分为三份，一份训练，一份调参，一份测试
    numPartitions = 4
    training = ratingRDD.filter(lambda x: x[0] < 6) \
      .values() \
      .repartition(numPartitions) \
      .cache()

    validation = ratingRDD.filter(lambda x: x[0] >= 6 and x[0] < 8) \
      .values() \
      .repartition(numPartitions) \
      .cache()

    test = ratingRDD.filter(lambda x: x[0] >= 8).values().cache()

    numTraining = training.count()
    numValidation = validation.count()
    numTest = test.count()
    print "numTraining:%d, numValidation:%d, numTest:%d" % (numTraining, numValidation, numTest)
    model = train(training=training, validation=validation, test=test)
    predict(model, test, int(sys.argv[3])) 
    sc.stop()
