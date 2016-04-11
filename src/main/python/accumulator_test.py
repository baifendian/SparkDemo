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

from pyspark import SparkConf, SparkContext

'''
统计包含wenting， dongshen， qifeng 个数 
'''

# 需要设置成global， 才能更改main中的4个accumulator
def fun(element):
    global a_wenting, a_dongshen, a_qifeng, a_else
    if "wenting" in element:
        a_wenting += 1
    elif "dongshen" in element:
        a_dongshen += 1
    elif "qifeng" in element:
        a_qifeng += 1
    else:
        a_else += 1
    return element


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: input <file>")
        exit(-1)

    sc = SparkContext(appName="accumulatorTest")
    # 创建四个统计要素
    a_wenting = sc.accumulator(0)
    a_dongshen = sc.accumulator(0)
    a_qifeng= sc.accumulator(0)
    a_else= sc.accumulator(0)

    lines = sc.textFile(sys.argv[1], 2).map(fun)

    for line in lines.collect():
        print line
    print a_wenting.value
    print a_else.value

    sc.stop()
