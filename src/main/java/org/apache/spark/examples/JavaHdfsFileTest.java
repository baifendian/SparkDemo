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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hdfs 文件操作
 * <p>
 * 
 * @author : dsfan
 * @date : 2016年3月11日
 */
public class JavaHdfsFileTest {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: JavaHdfsFileTest <localFile> <dfsDir>");
            System.exit(1);
        }
        String localFile = args[0];
        String dfsDir = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("JavaHdfsFileTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<String> lineList = readLocalFile(localFile);

        if (!lineList.isEmpty()) {
            printList("The local file's content is :", lineList);
            jsc.parallelize(lineList).saveAsTextFile(dfsDir);

            // 读取hdfs中的文件
            JavaRDD<String> lines = jsc.textFile(dfsDir);
            printList("The hdfs file's content is :", lines.collect());
        } else {
            System.out.println("Error: The local file is Empty!");
        }

        jsc.close();
    }

    /**
     * 读取本地文件（直接）
     * <p>
     *
     * @param localPath
     */
    private static List<String> readLocalFile(String localPath) {
        List<String> lineList = new ArrayList<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(localPath)));
            String temp = null;
            // 一次读一行，读入null时文件结束
            while ((temp = reader.readLine()) != null) {
                lineList.add(temp);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return lineList;
    }

    /**
     * 打印list内容
     * <p>
     *
     * @param title
     * @param lineList
     */
    private static void printList(String title, List<String> lineList) {
        System.out.println(title);
        for (String line : lineList) {
            System.out.println(line);
        }
    }
}
