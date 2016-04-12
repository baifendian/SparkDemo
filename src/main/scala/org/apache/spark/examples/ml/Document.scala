/**
 * Copyright 2016 BFD inc.
 * Author: qifeng.dai@baifendian.com
 * Create Time: 2016-04-12 11:08
 * Modify:
 * Desc:
 **/
package org.apache.spark.examples.ml

import scala.beans.BeanInfo

@BeanInfo
case class LabeledDocument(id: Long, text: String, label: Double)

@BeanInfo
case class Document(id: Long, text: String)
