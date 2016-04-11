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
from pyspark.sql import HiveContext


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: hive input file")
        exit(-1)

    path = sys.argv[1]

    conf = SparkConf().setAppName("spark_sql_hive")

    sc = SparkContext(conf=conf)

    hc = HiveContext(sc)

    # 创建表
    hc.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    # 加载数据
    hc.sql("LOAD DATA INPATH '%s' INTO TABLE src" % path)
    # 注册函数
    hc.registerFunction("myfunc", lambda name: name.upper())

    rows = hc.sql("select key, myfunc(value) from src").take(5)

    for row in rows:
        print row

    sc.stop()

