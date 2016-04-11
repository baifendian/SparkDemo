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
    sc = SparkContext(appName="PythonSQL")

    sqlContext = SQLContext(sc)

    '''********************1 通过row的方式创建dataframe******************'''
    #datas =["John 19", "Smith 23", "Sarah 18"]
    #source = sc.parallelize(datas)
    #splits = source.map(lambda line: line.split(""))
    #rows = splits.map(lambda words: Row(name=words[0], age=words[1])
    some_rdd = sc.parallelize([Row(name="John", age=19),
                              Row(name="Smith", age=23),
                              Row(name="Sarah", age=18)])
    some_df = sqlContext.createDataFrame(some_rdd)
    some_df.printSchema()

    '''********************2 通过schema 的方式创建dataframe******************'''
    another_rdd = sc.parallelize([("John", 19), ("Smith", 23), ("Sarah", 18)])
    schema = StructType([StructField("person_name", StringType(), False),
                        StructField("person_age", IntegerType(), False)])
    another_df = sqlContext.createDataFrame(another_rdd, schema)
    another_df.printSchema()

    '''********************3 读取文件的方式创建dataframe******************'''
    if len(sys.argv) < 2:
        print("Usage: json file or text file")
        exit(-1)
    path = sys.argv[1]
    if ".json" in path:
        people = sqlContext.read.json(path)
        people.printSchema()
    else:
        lines = sc.textFile(path)
        parts = lines.map(lambda l: l.split(","))
        people_data = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
        people = sqlContext.createDataFrame(people_data)

        # people_data = parts.map(lambda p: (p[0], p[1].strip()))
        # schemaString = "name age"
        # fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        # schema = StructType(fields)
        # people = sqlContext.createDataFrame(people_data, schema)

    people.registerTempTable("people")
    # SQL statements can be run by using the sql methods provided by sqlContext
    teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    for each in teenagers.collect():
        print(each[0])

    sc.stop()

