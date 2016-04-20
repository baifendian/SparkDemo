/**
 * Copyright 2016 BFD inc.
 * Author: qifeng.dai@baifendian.com
 * Create Time: 2016-04-15 10:05
 * Modify:
 * Desc:
 **/
package org.apache.spark.examples.ml

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object StringIndexer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StringIndexer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)



    sc.stop()
  }
}