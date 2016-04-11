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


if __name__ == "__main__":
    sc = SparkContext(appName="mysql_read_write")

    # use test
    # create table people(name varchar(20), age int)
    # insert people(name,age) values("wenting.wang", 28);
    # insert people(name,age) values("dongshen.fan", 26);

    url = "jdbc:mysql://172.18.1.22:3306/test"
    table = "people"
    properties = {"user": "hive", "password": "hive123"}
    sqlContext = SQLContext(sc)

    df = sqlContext.read.jdbc(url=url, table=table, properties=properties)
    df.printSchema()

    results = df.select("name", "age")
    names = results.map(lambda p: "Name: " + p.name)
    for name in names.collect():
        print(name)

    sc.stop()
