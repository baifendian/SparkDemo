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

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType


if __name__ == "__main__":
    sc = SparkContext(appName="parquet_read_write")

    if len(sys.argv) != 3:
        print("Usage: input file, output file")
        exit(-1)

    sqlContext = SQLContext(sc)
    # 创建schema
    schemaString = "name age"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    input = sys.argv[1]
    output = "%s/people.parquet" % sys.argv[2]
    # 创建dataframe
    df = sqlContext.read.json(sys.argv[1],schema)
    datas = df.select("name", "age")

    #保存parquet 文件(写入前确保文件不在hdfs上面)
    datas.write.parquet(output)

    #读取parquet文件
    parquetFile = sqlContext.read.parquet(output)
    parquetFile.registerTempTable("parquetFile");
    teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenNames = teenagers.map(lambda p: "Name: " + p.name)
    for teenName in teenNames.collect():
        print(teenName)



    sc.stop()

