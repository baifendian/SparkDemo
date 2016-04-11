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
def readFile():
    flag = 0
    file = open("dataset.txt", 'r')
    file_out1 = open("dataset_operator.txt", 'w')
    file_out2 = open('name.txt', 'w')
    for line in file:
        idx = 0
        items = line.split('\t')
        if flag == 0:
            flag = 1
            for item in items:
                file_out2.write(str(item) + " ")
        else:
            for item in items:
                if (item == 'T'):
                    file_out1.write(str(idx) + " ")
                idx = idx + 1
            file_out1.write('\n')
readFile()
