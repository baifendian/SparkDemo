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

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 累加器测试
 * <p>
 * 
 * @author : dsfan
 * @date : 2016年3月11日
 */
public class JavaAccumulatorTest {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("JavaAccumulatorTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 设置 "累加器"
        Accumulator<Integer> accum = jsc.accumulator(0);

        JavaRDD<Integer> dataSet = jsc.parallelize(Arrays.asList(1, 2, 3, 4));
        for (Integer data : dataSet.collect()) {
            accum.add(data);
        }

        System.out.println("Accumulator value is :" + accum.value());

        jsc.close();
    }
}
