#### 示例 1: 求 pi 的程序

##### 代码地址为：[SparkPi](/src/main/java/org/apache/spark/examples/JavaSparkPi.java)

代码提交方式如下:

```
[hadoop@hlg-2p239-fandongsheng spark]$ spark-submit --class org.apache.spark.examples.JavaSparkPi \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 512M \
                                        spark-examples_2.10-1.0-SNAPSHOT.jar 10

// 输出如下所示:
Pi is roughly 3.140832
```


#### 示例 2: 使用 "广播变量"

##### 代码地址为: [JavaBroadcastTest](/src/main/java/org/apache/spark/examples/JavaBroadcastTest.java)

提交方式为：

```
[hadoop@hlg-2p239-fandongsheng spark]$ spark-submit --class org.apache.spark.examples.JavaBroadcastTest \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples_2.10-1.0-SNAPSHOT.jar

// 输出如下所示：
Broadcast value is : 1 2 3
```


#### 示例 3: 使用 "累加器变量"

##### 代码地址为: [JavaAccumulatorTest](/src/main/java/org/apache/spark/examples/JavaAccumulatorTest.java)

提交方式为：

```
[hadoop@hlg-2p239-fandongsheng spark]$ spark-submit --class org.apache.spark.examples.JavaAccumulatorTest \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples_2.10-1.0-SNAPSHOT.jar

// 输出如下所示：
Accumulator value is :10
```


#### 示例 4: "读/写" 各种文件




