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
# 该示例说明如何在程序中加载使用R packages
library(SparkR)

# install.packages是R中用来动态安装R Packages的函数，在SparkR中调用这个函数会把对应的R Packages下载到$SPARK_HOME/R/lib目录，如果运行在Yarn模式下，lib目录下所有的R Packages会一起打包成$Spark_HOME/R/lib/sparkr.zip，并上传到相应的Yarn执行器中
install.packages("matlab", repos="http://mirror.bjtu.edu.cn/cran")

sc <- sparkR.init(appName="demo_package")

# 如果在SparkR:::lapply和SparkR:::lapplyPartition等函数中使用了R Packages，需要使用SparkR:::includePackage在spark executor上加载相应的包
SparkR:::includePackage(sc, matlab)

rdd <- SparkR:::parallelize(sc, 1:10, 2)

generateOnes <- function(x) {
  ones(5)
}

onesRDD <- SparkR:::lapplyPartition(rdd, generateOnes)

output = SparkR:::collect(onesRDD)

print(output)
