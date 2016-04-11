/**
 * Copyright (C) 2015 Baifendian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 操作本地文件
 * <p>
 * 
 * @author : dsfan
 * @date : 2016年3月11日
 */
public class JavaLocalFileTest {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: JavaHdfsFileTest <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaLocalFileTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("file://" + args[0]);

        for (String line : lines.collect()) {
            System.out.println(line);
        }

        jsc.close();
    }
}
