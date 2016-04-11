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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * 熟悉各种RDD的操作
 * <p>
 * 
 * @author : dsfan
 * @date : 2016年3月16日
 */
public class JavaRddTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaRddTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        List<String> list = new ArrayList<>();
        list.add("a,b,c,d,e");
        list.add("1,2,3,4,5");
        JavaRDD<String> rddString = jsc.parallelize(list);

        List<String> list2 = new ArrayList<>();
        list2.add("1,2,3,4,5");
        list2.add("aa,bb,cc,dd,ee");
        list2.add("11,22,33,44,55");
        JavaRDD<String> rddString2 = jsc.parallelize(list2);

        // map
        rddMap(rdd);

        // map 2
        rddMap2(rdd);

        // flatMap
        rddFlatMap(rddString);

        // filter
        rddFilter(rdd);

        // union
        rddUnion(rddString, rddString2);

        // mapToPair
        mapToPair(rddString);

    }

    /**
     * <p>
     *
     * @param rddString
     */
    private static void mapToPair(JavaRDD<String> rddString) {
        JavaPairRDD<String, String> pairRdd = rddString.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

            @Override
            public Iterable<Tuple2<String, String>> call(String s) throws Exception {
                String[] temp = s.split(",");
                ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                list.add(new Tuple2<String, String>(temp[0], temp[1]));
                return list;
            }
        });
        for (Tuple2 tuple2 : pairRdd.collect()) {
            System.out.print(" " + tuple2._1() + ":" + tuple2._2());
        }
        System.out.println();
    }

    /**
     * <p>
     *
     * @param rddString
     * @param rddString2
     */
    private static void rddUnion(JavaRDD<String> rddString, JavaRDD<String> rddString2) {
        JavaRDD<String> unionRdd = rddString.union(rddString2);
        for (String t : unionRdd.collect()) {
            System.out.print(" " + t + " ");
        }
        System.out.println();
    }

    /**
     * <p>
     *
     * @param rdd
     */
    private static void rddFilter(JavaRDD<Integer> rdd) {
        JavaRDD<Integer> filterRdd = rdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        for (Integer value : filterRdd.collect()) {
            System.out.print(" " + value);
        }
        System.out.println();
    }

    /**
     * <p>
     *
     * @param rddString
     */
    private static void rddFlatMap(JavaRDD<String> rddString) {
        JavaRDD<String> flatMapRdd = rddString.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(","));
            }
        });

        for (String t : flatMapRdd.collect()) {
            System.out.print(" " + t + " ");
        }
        System.out.println();
    }

    /**
     * <p>
     *
     * @param rdd
     */
    private static void rddMap2(JavaRDD<Integer> rdd) {
        JavaRDD<Tuple2> mapRdd2 = rdd.map(new Function<Integer, Tuple2>() {
            @Override
            public Tuple2 call(Integer v1) throws Exception {
                return new Tuple2(v1, 2 * v1);
            }
        });

        for (Tuple2 tuple2 : mapRdd2.collect()) {
            System.out.print(" " + tuple2._1() + ":" + tuple2._2());
        }
        System.out.println();
    }

    /**
     * <p>
     *
     * @param rdd
     */
    private static void rddMap(JavaRDD<Integer> rdd) {
        JavaRDD<Integer> mapRdd = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        for (Integer value : mapRdd.collect()) {
            System.out.print(" " + value);
        }
        System.out.println();
    }
}
