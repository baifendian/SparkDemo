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
# 该示例说明如何在sparkR中使用ml相关算法（广义线性模型）

# 初始化sparkR和sqlContext环境
sc <- sparkR.init(master="local[2]", appName="ml_glm_example")
sqlContext <- sparkRSQL.init(sc)

# 加载iris数据集为DataFrame
df <- createDataFrame(sqlContext, iris)

# 训练gaussian glm模型
model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")

# 输出模型结果
summary(model)

# 模型预测
predictions <- predict(model, newData = df)
head(select(predictions, "Sepal_Length", "prediction"))

# 清理训练集
training <- filter(df, df$Species != "setosa")
test <- training

# 训练binomial gaussian模型
model <- glm(Species ~ Sepal_Length + Sepal_Width, data = training, family = "binomial")

# 输出模型结果
summary(model)

# 模型预测
predictions <- predict(model, test)
head(select(predictions, "Species", "prediction"))

# 关闭sparkR环境
sparkR.stop()

