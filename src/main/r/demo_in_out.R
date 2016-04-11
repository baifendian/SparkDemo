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
# 该示例用于说明sparkR如何加载和导出数据

# 加载sparkR库
library(SparkR)

# 初始化sc和sqlContext环境
sc <- sparkR.init(master="local[2]", appName="in_out_example")
sqlContext <- sparkRSQL.init(sc)

# R中data frames转化为spark的DataFrame
df_list <- createDataFrame(sqlContext, list(1,2,3,4))
print("local example")
#
name <- c("Tom", "Helen", "Jack")
age <- c(20, 35, 20)
sex <- c("m", "f", "m")
employees <- data.frame(name, age, sex)
df_employees <- createDataFrame(sqlContext, employees)
printSchema(df_employees)
head(df_employees)
#
df <- createDataFrame(sqlContext, faithful)
head(df)

# 读取数据文件, (路径为HDFS上位置)
# JSON格式，方法一
path <- file.path("/user/bre/xiang.xu/people.json")
peopleDF <- jsonFile(sqlContext, path)
# JSON格式，方法二
peopleDF <- read.df(sqlContext, "/user/bre/xiang.xu/people.json", "json")
printSchema(peopleDF)
head(peopleDF)
# csv格式


# 读取hive中的表，执行HQL语句
hiveContext <- sparkRHive.init(sc)
#sql(hiveContext, "CREATE TABLE IF NOT EXISTS spark_test (key INT, value STRING)")
#sql(hiveContext, "LOAD DATA INPATH '/user/bre/xiang.xu/kv1.txt' INTO TABLE spark_test")
results <- sql(hiveContext, "FROM spark_test SELECT key, value")
head(results)

# external databases


# 数据输出到HDFS上，方法一
write.df(df, path="/user/bre/xiang.xu/df1.parquet", source="parquet", mode="error")
# 方法二
saveDF(df, path="/user/bre/xiang.xu/df2.parquet")

# 关闭sc环境
sparkR.stop()

