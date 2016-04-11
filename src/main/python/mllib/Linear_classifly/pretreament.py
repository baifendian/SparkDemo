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
import os
import numpy as np
file = open(sys.argv[1], "r")
file_out = open(sys.argv[2], "w")
# age, fnlwgt, education-num,* Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse *Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving Priv-house-serv, Protective-serv, Armed-Forces * White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black * Female, Male * capital-gain * capital-loss * hours-per-week* 
dict_marital_status = {"Married-civ-spouse": 3, "Divorced": 4, "Never-married": 5, "Separated": 6, "Widowed": 7, "Married-spouse-absent": 8, "Married-AF-spouse": 9}

dict_occupation = {"Tech-support": 10, "Craft-repair": 11, "Other-service": 12, "Sales": 13, "Exec-managerial": 14, "Prof-specialty": 15, "Handlers-cleaners": 16, "Machine-op-inspct": 17, "Adm-clerical": 18, "Farming-fishing": 19, "Transport-moving": 20, "Priv-house-serv": 21, "Protective-serv": 22, "Armed-Forces": 23}

dict_race = {"White": 24, "Asian-Pac-Islander": 25, "Amer-Indian-Eskimo": 26, "Other": 27, "Black": 28}
dict_sex = {"Female": 29, "Male": 30}
idx = 0
for line in file:
    print idx
    idx += 1
    featrue = np.zeros(35, dtype=float)
    data = line.split(", ")
    if (len(data) < 15):
        print "*"
        continue
    if (data[0] == '?' or data[2] == '?' or  data[4] == '?' or  data[5] == '?' or data[6] == '?' or data[8] == '?' or data[9] == '?' or data[10] == '?' or data[11] == '?' or data[12] == '?'):
        continue
    try:
        featrue[0] = float(data[0])
        featrue[1] = float(data[2])
        featrue[2] = float(data[4])
        featrue[dict_marital_status[data[5]]] = 1
        featrue[dict_occupation[data[6]]] = 1
        featrue[dict_race[data[8]]] = 1
        featrue[dict_sex[data[9]]] = 1
        featrue[31] = float(data[10])
        featrue[32] = float(data[11])
        featrue[33] = float(data[12])
        featrue[34] = 0 if (data[14] == "<=50K\n") else 1
        file_out.write(str(featrue.tolist()) + "\n")
    except Exception, e:
        pass

    
