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
import json
from functools import partial

from pyspark import SparkConf, SparkContext

# 过滤掉不包含wenting， dongshen， qifeng 的输入
def fun(element):
    for value in bc.value:
        if value["name"] in element:
            return True
    return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: input <file>")
        exit(-1)
    # 加载broadcast.txt 文件 [{"name":"wenting", "id":101}, {"name":"dongshen","id":102}, {"name":"qifeng", "id":103}]

    data = json.load(open("broadcast.txt"))
    sc = SparkContext(appName="broadcastTest")
    # 将data 设置成broadcast
    bc = sc.broadcast(data)

    lines = sc.textFile(sys.argv[1]).filter(fun)
    for line in lines.collect():
        print line
        
    sc.stop()
