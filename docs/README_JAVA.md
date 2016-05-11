## Java 程序示例
### 示例 1: 求 pi 的程序

##### 代码地址为：[SparkPi](/src/main/java/org/apache/spark/examples/JavaSparkPi.java)

代码提交方式如下:

```
$ spark-submit --class org.apache.spark.examples.JavaSparkPi \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 512M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 10

// 输出如下所示:
Pi is roughly 3.140832
```


### 示例 2: 使用 "广播变量"

##### 代码地址为: [JavaBroadcastTest](/src/main/java/org/apache/spark/examples/JavaBroadcastTest.java)

提交方式为：

```
$ spark-submit --class org.apache.spark.examples.JavaBroadcastTest \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

// 输出如下所示：
Broadcast value is : 1 2 3
```


### 示例 3: 使用 "累加器变量"

##### 代码地址为: [JavaAccumulatorTest](/src/main/java/org/apache/spark/examples/JavaAccumulatorTest.java)

提交方式为：

```
$ spark-submit --class org.apache.spark.examples.JavaAccumulatorTest \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

// 输出如下所示：
Accumulator value is :10
```


### 示例 4: "读/写" 各种文件


#### 1. 读取本地文件: [JavaLocalFileTest](/src/main/java/org/apache/spark/examples/JavaLocalFileTest.java)

提交方式:

```
$ spark-submit --class org.apache.spark.examples.JavaLocalFileTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /etc/sysconfig/network
```

#### 2. 读写文本文件的例子(从本地读取写入到 HDFS 上): [JavaHdfsFileTest](/src/main/java/org/apache/spark/examples/JavaHdfsFileTest.java)

提交方式:

```
$ spark-submit --class org.apache.spark.examples.JavaHdfsFileTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /etc/sysconfig/network /user/spark/output/network
```

#### 3. 读写 JSON 文件的例子: [JavaJsonTest](/src/main/java/org/apache/spark/examples/JavaJsonTest.java)


该代码展示了如何读取一个 JSON 的示例，用到的数据是真实数据，样例如下:
```
{"api_category_name":"UserAction","appkey":"8fda5524dfc4af820f0f68d557381d72","bt":"Chrome46","callback":"BCore.instances[2].callbacks[0]","cb":["C"],"cid":"Cwangfujing","ct":"utf-8","d_s":"pc","ep":"http://172.18.1.22:3033/","fv":"19.0 r0","gid":"87205254007bf9520000031700000001565c2d1c","ip":"172.18.1.22","is_newgid":false,"item_type":"ItemBase","ja":true,"lt":10000,"method":"PageView","oc":"zh-cn","ot":"Windows NT 4.0","p_id":"aa","p_p":"a","p_s":"b","p_t":"hp","ptime":111,"random":"1448881467714","ref_page":"","rs":[1366,768],"sid":"255135432.70309399.1448881462059","terminal":"PC","timestamp":1448933047.0550001,"tma":"255135432.70330821.1448881462063.1448881462063.1448881462063.1","tmc":"2.255135432.70330821.1448881462063.1448881462063.1448881467404","tmd":"2.255135432.70330821.1448881462063.","uid":"255135432.70309399.1448881462059","user_agent":"Apache-HttpClient/4.2.6 (java 1.5)","uuid":"Input:87205254007bf952:0000033d:00037805:565cf6b7"}
```


代码提交方式如下:

```
$ hadoop fs -rm -r /user/spark/output/result

$ spark-submit --class org.apache.spark.examples.JavaJsonTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/spark/input/useraction.json /user/spark/output/result
```

#### 4. 读取 CSV 文件的例子: [JavaParseCsvTest](/src/main/java/org/apache/spark/examples/JavaParseCsvTest.java)

CSV 文件格式也是一种比较常见的文件格式，是以 "," 分隔的文件，每一行包含了固定数目的字段，对于 CSV 文件的加载和 JSON 文件格式类似，也有很多 package 支持。

代码提交方式如下:

```
$ hadoop fs -rm -r /user/spark/output/result

$ spark-submit --class org.apache.spark.examples.JavaParseCsvTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/spark/input/useraction.csv /user/spark/output/result
```

#### 5. 读取 SequenceFiles 文件的例子: [JavaParseSequenceFiles](/src/main/java/org/apache/spark/examples/JavaParseSequenceFiles.java)

SequenceFiles 是一种非常流行的 Hadoop format，它包含 key/value pairs，它具有同步标记用来对文件中的记录进行 "切割". 这种特性也使得 Spark 能在多个结点并行处理它。


代码提交方式如下:

```
$ spark-submit --class org.apache.spark.examples.JavaParseSequenceFiles \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/spark/input/sample.seq
```

### 示例 5: 使用 JDBC 接口 [JavaJdbcTest](/src/main/java/org/apache/spark/examples/JavaJdbcTest.java)

Spark 能够访问一些数据库，比如从关系数据库加载数据(使用 JDBC)，包括 MySQL，Postgres 等等。

代码提交方式如下:

```
$ spark-submit --class org.apache.spark.examples.LoadByJDBC \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```
### 示例6. SQL

##### 代码地址为: [JavaSparkSQL](/src/main/java/org/apache/spark/examples/sql/JavaSparkSQL.java)

代码提交方式如下:

```
$ spark-submit --class org.apache.spark.examples.sql.JavaSparkSQL \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

### 示例7. Streaming

#### 1 Kafka 单词计数 [JavaKafkaWordCount](/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java)

这个例子主要讲解了读取 Kafka 中的数据，然后进行单词计算统计的过程，其中直接通过命令行向 kafka 中写入数据。

首先启动发送单词的命令行：

```
$ ./kafka-console-producer.sh --broker-list 172.18.1.22:9092,172.18.1.23:9092,172.18.1.24:9092 --topic topic1
```

然后我们启动 JavaKafkaWordCount 用来统计一下单词的个数情况：

```
$ spark-submit --class org.apache.spark.examples.streaming.JavaKafkaWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22:2181,172.18.1.23:2181,172.18.1.24:2181/kafka_0_8_2_1 my-consumer-group1 topic1 2
```

#### 2 Kafka 单词计数-Direct 接口版本 [JavaDirectKafkaWordCount](/src/main/java/org/apache/spark/examples/streaming/JavaDirectKafkaWordCount.java)

首先启动发送单词的命令行：

```
$ ./kafka-console-producer.sh --broker-list 172.18.1.22:9092,172.18.1.23:9092,172.18.1.24:9092 --topic topic2
```

然后启动统计程序：

```
$ spark-submit --class org.apache.spark.examples.streaming.JavaDirectKafkaWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22:9092,172.18.1.23:9092,172.18.1.24:9092 topic2
```

#### 3 网络发送单词的计数问题 [JavaNetworkWordCount](/src/main/java/org/apache/spark/examples/streaming/JavaNetworkWordCount.java)

启动网络端口，并且往里面发送数据：

```
$ nc -lk 9999

hello world
...
```

启动网络计数的程序：

```
$ spark-submit --class org.apache.spark.examples.streaming.JavaNetworkWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22 9999
```

#### 4 网络发送单词的计数问题-CheckPoint版本 [JavaRecoverableNetworkWordCount](/src/main/java/org/apache/spark/examples/streaming/JavaRecoverableNetworkWordCount.java)

启动网络端口，并且往里面发送数据：

```
$ nc -lk 9999

hello world
...
```

启动网络计数的程序，这个程序会统计单词的个数，并且将计数信息写到 Redis 中：

```
$ spark-submit --class org.apache.spark.examples.streaming.JavaRecoverableNetworkWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22 9999 /user/spark/checkpoint 172.18.1.22 6379
```

#### 5 网络发送单词的计数问题-SQL版本 [JavaSqlNetworkWordCount](/src/main/java/org/apache/spark/examples/streaming/JavaSqlNetworkWordCount.java)

启动网络端口，并且往里面发送数据：

```
$ nc -lk 9999

hello world
...
```

启动网络计数的程序，通过 SQL 统计单词个数：

```
$ spark-submit --class org.apache.spark.examples.streaming.JavaSqlNetworkWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22 9999
```