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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量测试
 * <p>
 * 
 * @author : dsfan
 * @date : 2016年3月11日
 */
public class JavaBroadcastTest {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("JavaBroadcastTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 设置 "广播变量"
        Broadcast<int[]> broadcastVar = jsc.broadcast(new int[] { 1, 2, 3 });

        System.out.print("Broadcast value is :");
        for (int value : broadcastVar.value()) {
            System.out.print(" " + value);
        }
        System.out.println();

        jsc.close();
    }
}
