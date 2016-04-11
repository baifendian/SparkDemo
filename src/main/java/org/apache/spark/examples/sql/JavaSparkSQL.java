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

package org.apache.spark.examples.sql;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * spark sql测试
 * <p>
 * 
 * @author : dsfan
 * @date : 2016年3月22日
 */
public class JavaSparkSQL {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(ctx);

        // test text
        DataFrame schemaPeople = testTextDataset(ctx, sqlContext);

        // test parquest
        testParquestDataset(sqlContext, schemaPeople);

        // test json
        testJsonDataset(ctx, sqlContext);

        // test Hive
        testHiveDataset(ctx);

        // test JDBC
        testJdbcDataset(ctx, sqlContext);

        ctx.stop();
    }

    /**
     * Data source: RDD
     * <p>
     *
     * @param ctx
     * @param sqlContext
     * @return
     */
    private static DataFrame testTextDataset(JavaSparkContext ctx, SQLContext sqlContext) {
        System.out.println("=== Data source: RDD ===");
        // 数据源为txt文本
        // 加载txt文件，文件在hdfs中
        JavaRDD<Person> people = ctx.textFile("/tmp/examples/people.txt").map(new Function<String, Person>() {
            @Override
            public Person call(String line) {
                String[] parts = line.split(",");

                Person person = new Person();
                person.setName(parts[0]);
                person.setAge(Integer.parseInt(parts[1].trim()));

                return person;
            }
        });

        // 创建一个DataFrame，并将其注册为一个Table
        DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
        schemaPeople.registerTempTable("people");

        // SQL查询，筛选条件(age >= 13 AND age <= 19)
        DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        // 处理（名称前加"Name:"）并打印结果
        dataframe2RddAndPrint(teenagers);

        return schemaPeople; // 供后续使用
    }

    /**
     * Data source: Parquet File
     * <p>
     *
     * @param sqlContext
     * @param schemaPeople
     */
    private static void testParquestDataset(SQLContext sqlContext, DataFrame schemaPeople) {
        System.out.println("=== Data source: Parquet File ===");
        // 数据源为parquet文件
        // 将上面的DataFrame查询报错为 parquet 文件
        schemaPeople.write().parquet("people.parquet");

        // 再将刚刚存储的parquet文件读取出来
        DataFrame parquetFile = sqlContext.read().parquet("people.parquet");

        // 将parquetFile注册为一个Table
        parquetFile.registerTempTable("parquetFile");

        // 执行SQL查询
        DataFrame teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");

        // 处理（名称前加"Name:"）并打印结果
        dataframe2RddAndPrint(teenagers);
    }

    /**
     * Data source: JSON Dataset
     * <p>
     *
     * @param ctx
     * @param sqlContext
     */
    private static void testJsonDataset(JavaSparkContext ctx, SQLContext sqlContext) {
        System.out.println("=== Data source: JSON Dataset ===");
        // 数据源为json文件
        String path = "/tmp/examples/people.json"; // hdfs中
        // 从json数据源创建DataFrame
        DataFrame peopleFromJsonFile = sqlContext.read().json(path);

        // 由于json格式的文件能够直接推断出数据结构，所以我们直接打印下看看
        peopleFromJsonFile.printSchema();
        // 打印如下：
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        // 将DataFrame注册为一个Table
        peopleFromJsonFile.registerTempTable("people");

        // 执行SQL查询
        DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
        // 处理（名称前加"Name:"）并打印结果
        dataframe2RddAndPrint(teenagers);

        // 换一个json数据结构的例子
        List<String> jsonData = Arrays.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        JavaRDD<String> anotherPeopleRDD = ctx.parallelize(jsonData);
        DataFrame peopleFromJsonRDD = sqlContext.read().json(anotherPeopleRDD.rdd());

        // 打印出新的数据结构
        peopleFromJsonRDD.printSchema();
        // 打印如下：
        // root
        // |-- address: struct (nullable = true)
        // | |-- city: string (nullable = true)
        // | |-- state: string (nullable = true)
        // |-- name: string (nullable = true)

        // 将DataFrame注册为一个Table
        peopleFromJsonRDD.registerTempTable("people2");

        // 执行SQL查询
        DataFrame peopleWithCity = sqlContext.sql("SELECT name, address.city FROM people2");
        List<String> nameAndCity = peopleWithCity.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0) + ", City: " + row.getString(1);
            }
        }).collect();
        for (String name : nameAndCity) {
            System.out.println(name);
        }
    }

    /**
     * Data source: JDBC Dataset(以MySQL为例)
     * <p>
     *
     * @param ctx
     * @param sqlContext
     */
    private static void testJdbcDataset(JavaSparkContext ctx, SQLContext sqlContext) {
        System.out.println("=== Data source: JDBC Dataset(以MySQL为例) ===");
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://172.18.1.22:3306/test");
        options.put("dbtable", "people");
        options.put("user", "hive");
        options.put("password", "hive123");

        DataFrame jdbcDF = sqlContext.read().format("jdbc").options(options).load();
        jdbcDF.registerTempTable("people");
        DataFrame teenagers = jdbcDF.sqlContext().sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
        // 处理（名称前加"Name:"）并打印结果
        dataframe2RddAndPrint(teenagers);
    }

    /**
     * Data source: Hive Dataset
     * <p>
     *
     * @param ctx
     */
    private static void testHiveDataset(JavaSparkContext ctx) {
        System.out.println("=== Data source: Hive Dataset ===");
        HiveContext hiveContext = new HiveContext(ctx);
        DataFrame teenagers = hiveContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
        dataframe2RddAndPrint(teenagers);
    }

    /**
     * dataframe转化为rdd并输出结果
     * <p>
     *
     * @param teenagers4
     */
    private static void dataframe2RddAndPrint(DataFrame teenagers4) {
        List<String> teenagerNames = teenagers4.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        for (String name : teenagerNames) {
            System.out.println(name);
        }
    }

    public static class Person implements Serializable {
        private String name;

        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
