##
# Copyright (C) 2015 Baifendian Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##
# 该示例说明如何调用 RDD API
# 注意：由于SparkR Package中没有export RDD相关的函数，所以只能使用R的语法:::来调用RDD API。

library(SparkR)

# 输入参数处理
args <- commandArgs(trailing=TRUE)
if(length(args) != 1) {
    print("Usage: demo_wordcount <input>")
    q("no")
}

# 初始化环境
sc <- sparkR.init(appName="demo_wc")

# 将文件内容读取到集群中
rdd <- SparkR:::textFile(sc, args[[1]])

words <- SparkR:::flatMap(rdd, function(line){
     strsplit(line, " ")[[1]]
})

# 类似于Python中的map函数
wordcount <- SparkR:::lapply(words, function(word){
     list(word, 1L)
})


counts <- SparkR:::reduceByKey(wordcount, "+", 2L)
output <- SparkR:::collect(counts)

# 结果输出
for (wordcount in output) {
  cat(wordcount[[1]], ": ", wordcount[[2]], "\n")
}

sparkR.stop()

