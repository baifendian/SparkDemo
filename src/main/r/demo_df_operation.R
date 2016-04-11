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
# 该示例说明如何在sparkR中对DataFrame进行操作

library(SparkR)

# 初始化sc和sqlContext环境
sc <- sparkR.init(master="local[2]", appName="dataframe_example")
sqlContext <- sparkRSQL.init(sc)

# 创建DataFrame
df <- createDataFrame(sqlContext, faithful)

# 选择列
w1 = select(df, df$waiting)
head(w1)
w2 = select(df, "waiting")
head(w2)

# 过滤行
w3 = filter(df, df$waiting<50)
head(w3)
w4 = filter(df, "waiting<50")
head(w4)

# 聚合和排序
waiting_counts = summarize(groupBy(df, df$waiting), count = n(df$waiting))
counts_sort = arrange(waiting_counts, desc(waiting_counts$count))
head(counts_sort)

# 注册临时表，使用SQL查询
registerTempTable(df, "faithful")
longtime = sql(sqlContext, "SELECT eruptions, waiting From faithful where eruptions>3 AND waiting>90")
ltdf <- collect(longtime)
print(ltdf)

# 关闭sparkR环境
sparkR.stop()
