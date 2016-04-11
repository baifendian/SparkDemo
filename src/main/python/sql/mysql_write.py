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
from pyspark.sql.types import StructField, StructType, StringType

# use test
# create table people(name varchar(20), age int)


if __name__ == "__main__":
    sc = SparkContext(appName="mysql_write")

    if len(sys.argv) != 2:
        print("Usage: input file<file>")
        exit(-1)
    url = "jdbc:mysql://172.18.1.22:3306/test"
    table = "people"
    properties = {"user": "hive", "password": "hive123"}
    # append overwrite
    mode = "append"

    sqlContext = SQLContext(sc)
    schemaString = "name age"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    df = sqlContext.read.json(sys.argv[1],schema)
    datas = df.select("name","age")

    datas.write.jdbc(url=url, table=table, mode=mode, properties=properties)


    sc.stop()
