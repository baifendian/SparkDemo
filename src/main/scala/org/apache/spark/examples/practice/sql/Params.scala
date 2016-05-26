/**
  * Copyright (C) 2015 Baifendian Corporation
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.spark.examples.practice.sql

import org.apache.spark.examples.practice.utils.ConfigParser

object Params {
  // 相关参数配置
  val days = ConfigParser.getInstance.getInt("days")

  val sourcedataDir = ConfigParser.getInstance.getString("sourcedata.dir")
  val sourcedataPartition = ConfigParser.getInstance.getInt("sourcedata.partition")

  val hiveStatdayDelete = ConfigParser.getInstance.getBoolean("hive.statday.delete")
  val hiveWarehouseDir = ConfigParser.getInstance.getString("hive.warehousedir")

  val hiveAlluserTable = ConfigParser.getInstance.getString("hive.alluser.table")
  val hiveAlluserCreateTable = ConfigParser.getInstance.getString("hive.alluser.createtable")

  val hiveNewuserTable = ConfigParser.getInstance.getString("hive.newuser.table")
  val hiveNewuserCreateTable = ConfigParser.getInstance.getString("hive.newuser.createtable")

  val mongodbHost = ConfigParser.getInstance.getString("mongodb.host")
  val mongodbPort = ConfigParser.getInstance.getInt("mongodb.port")
  val mongodbDB = ConfigParser.getInstance.getString("mongodb.db")
  val mongodbCollection = ConfigParser.getInstance.getString("mongodb.collection")

  // 对参数进行格式化处理返回
  override def toString(): String = {
    ConfigParser.getInstance.toString()
  }
}