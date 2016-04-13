## Python 程序示例

### 示例 1: wordcount  的程序
#### 代码地址为：[wordcount](/src/main/python/wordcount.py)
技术点:

    (1)设置conf参数，例如:spark.default.parallelism 在shuffer中的作用

    (2)描述了读取hdfs， 怎么设置读取的partition数目以及partition和hdfs文件块的关系

    (3)flatMap 的用法

    (4)reduceByKey 的用法以及shuffer的partition数目获取和partition函数的用法

代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 2 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               wordcount.py /user/baseline/input/input.txt
// 输出如下所示:
textfile partitions: 2
shuffer partitions:2
adsfdfdsafdaklfd: 1
just: 1
addfdfdaf: 1
nihao: 1
test: 1
world: 1
hello2: 1
hello1: 1
world1: 1
qifeng.dai: 4
helloworld: 1
hello3: 3
nihao3: 1
hello: 1
dafadlkfjdalfjald: 1
nihao2: 1
nihao1: 1
wenting.wang: 6
dongshen.fan: 2
```

### 示例 2: broadcast_test  的程序

#### 代码地址为：[broadcast](/src/main/python/broadcast_test.py)
技术点:

    (1)--files 的使用

    (2)broadcast 的使用

代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --files  broadcast.txt \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 2 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               broadcast_test.py /user/baseline/input/input.txt
// 输出如下所示:
wenting.wang hello                                                              
wenting.wang hello1
wenting.wang hello2
wenting.wang hello3
wenting.wang hello3
wenting.wang hello3
dongshen.fan world
dongshen.fan world1
qifeng.dai nihao
qifeng.dai nihao1
qifeng.dai nihao2
qifeng.dai nihao3
```

### 示例 3: accumulator_test  的程序

#### 代码地址为：[accumulator](/src/main/python/accumulator_test.py)

技术点:

    (1)accumulator 的使用

代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 2 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               accumulator_test.py /user/baseline/input/input.txt
// 输出如下所示:
wenting.wang hello                                                              
wenting.wang hello1
wenting.wang hello2
wenting.wang hello3
wenting.wang hello3
wenting.wang hello3
dongshen.fan world
dongshen.fan world1
qifeng.dai nihao
qifeng.dai nihao1
qifeng.dai nihao2
qifeng.dai nihao3
helloworld just test
addfdfdaf
adsfdfdsafdaklfd
dafadlkfjdalfjald
6
4
```

### 示例 4: pack  的程序

主要讲解python打包的用法

#### 代码地址为：[pack](/src/main/python/pack/process.py)

技术点:

    (1)打包: cd pack; zip -r pack.zip *

    (2)--files 的用法

    (3)--py-files 的用法

    (4)读取hbase数据，每个partition只初始化hbase client 一次

    (5)mapPartitions 的用法

日志结果查看:
    
    此样例中，设置了4个partition，在spark history中可以看出
    init dataprocess 只有4条记录， 代表hbase client只在每个partition中初始化一次


代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --files process.conf \
                                               --py-files pack.zip \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               process.py /user/baseline/input/input.txt
// 输出如下所示:
30                                                                              
6
2
```

### 示例 5. "读/写" 数据

Spark 支持各种数据源，包括 Local-FileSystem，HDFS，HBase，sequenceFile, Avro,JDBC databases 等
对于avro 和 JDBC 建议通过spark-sql 来操作。

#### 1. TextFile本地读取和hdfs中读取: [TextFile](/src/main/python/text_file.py)
技术点:

    (1) textFile本地读取 

    (2) HadoopFile 和 newAPIHadoopFile 的使用

代码提交方式如下:

```
读取本地文件(--master local)
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master local \
                                               text_file.py /home/baseline/wenting.wang/spark/input.txt local

读取hdfs 文件
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               text_file.py /user/baseline/input/input.txt hdfs
```

#### 2. sequencefilehdfs中读取: [SequenceFile](/src/main/python/sequencefile.py)
技术点:

    (1) sequencefile 读取和写入

    (2) newAPIHadoopFile 和 saveAsNewAPIHadoopFile 的使用

代码提交方式如下:
```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               sequencefile.py /user/baseline/input/sample.seq /user/baseline/output/sequence

```

### 示例 6. spark-sql 操作

spark-sql 支持各种数据源，textfile, json, parquet, avro, mysql, hive

#### 1. 读取json 文件 写 parquet文件:[parquet](/src/main/python/sql/parquet.py)
技术点:

    (1) json 文件读取，加载schema

    (2) sql 操作和 parquetfile读和写

代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               parquent.py /user/baseline/input/people.json /user/baseline/people.parquet

// 输出如下所示:
Name: Justin                                  
```

#### 2. spark-sql 的基本操作:[spark-sql](/src/main/python/sql/sql.py)
技术点:

    (1) row 的使用
    (2) schema的默认和显示创建

代码提交方式如下:
```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               sql.py /user/baseline/input/people.json

// 输出如下所示:
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)

root
 |-- person_name: string (nullable = false)
 |-- person_age: integer (nullable = false)

root                                                                            
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)

Justin                                
```

#### 3. mysql 的读取:[mysql-write](/src/main/python/sql/mysql_write.py)
技术点:

    (1) jdbc 写 mysql

代码提交方式如下:
```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-library-path /home/bfd_hz/hive/lib/mysql-connector-java-5.1.35-bin.jar \
                                               --jars /home/bfd_hz/hive/lib/mysql-connector-java-5.1.35-bin.jar \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               mysql_write.py /user/baseline/input/people.json

// 输出如下所示:

执行前:

mysql> select * from people;
Empty set (0.00 sec)

执行后:

mysql> select * from people;
+---------+------+
| name    | age  |
+---------+------+
| Michael |   50 |
| Justin  |   19 |
| Andy    |   30 |
+---------+------+
3 rows in set (0.00 sec)
```

#### 4. mysql 的读取:[mysql-read](/src/main/python/sql/mysql_read.py)
技术点:

    (1) jdbc 的读取 mysql

代码提交方式如下:
```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-library-path /home/bfd_hz/hive/lib/mysql-connector-java-5.1.35-bin.jar \
                                               --jars /home/bfd_hz/hive/lib/mysql-connector-java-5.1.35-bin.jar \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               mysql_read.py

// 输出如下所示:
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)

Name: wenting.wang
Name: dongshen.fan
Name: Michael
Name: Andy
Name: Justin
Name: Michael
Name: Andy
Name: Justin
```

#### 5. hive 创表和载入数据:[hive-operation](/src/main/python/sql/hive_sql.py)
技术点:

    (1) hive 创表和载入数据
    (2) hive 语句中的函数调用

代码提交方式如下:
```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               hive_sql.py /user/baseline/input/kv1.txt

// 输出如下所示:
Row(key=238, _c1=u'VAL_238')
Row(key=86, _c1=u'VAL_86')
Row(key=311, _c1=u'VAL_311')
Row(key=27, _c1=u'VAL_27')
Row(key=165, _c1=u'VAL_165')
```

### 示例7. Streaming

本实例主要讲解spark streaming 的Dstreams的创建,Dstreams的基本转换，Dstreams的transform
怎么进行checkpoint容错,windows一些操作

最后讲解streaming kafka 的Receiver-based读取 和 Direct读取 以及offset 设置的一些操作

执行请下载对应的jar包:
http://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-assembly_2.10/1.6.0 对应spark1.6.0

可以具体找spark版本对应的包

#### 1. 简单的streaming操作:[network_wordcount](/src/main/python/streaming/network_wordcount.py)
技术点:

    (1) 创建StreamingContext 设置batchDuration 

    (2) 通过socketTextStream 创建 Dstreams， 以及Dstreams 的foreachRDD操作

代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 1 \
                                               --driver-memory 512M \
                                               --num-executors 1 \
                                               --executor-cores 2 \
                                               --executor-memory 512M \
                                               --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar \
                                               network_wordcount.py 172.18.1.22 9999

// 输出如下所示:
服务端: 
nc -lk 9999
wenting
qifeng
wenting
dongshen
streaming输出:
(u'wenting', 1)
(u'qifeng', 1)
(u'wenting', 1)                                                                 
(u'dongshen', 1)      
                                 
```

#### 2. streaming+ spark sql操作:[sql_network_wordcount](/src/main/python/streaming/network_sql_wordcount.py)
技术点:

    (1) lazy的方式创建SQLContext

代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 2 \
                                               --driver-memory 2G \
                                               --num-executors 2 \
                                               --executor-cores 2 \
                                               --executor-memory 2G \
                                               --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar \
                                               network_sql_wordcount.py 172.18.1.22 9999

// 输出如下所示:
服务端: 
nc -lk 9999
wenting
qifeng
wenting
dongshen
streaming输出:
+--------+-----+                                                                
|    word|total|
+--------+-----+
|dongshen|    1|
|  qifeng|    1|
| wenting|    2|
+--------+-----+     
                                 
```

#### 3. 高级操作:[complex](/src/main/python/streaming/complex.py)
技术点:

    (1) lazy 的创建broadcast对象
    (2) lazy 的创建redis连接对象
    (3) foreachRDD 和 tansform的使用
    (4) foreachRDD 写到redis的优化
    (5) reduceByKeyAndWindow和checkpoint的使用

代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 2 \
                                               --driver-memory 4G \
                                               --num-executors 2 \
                                               --executor-cores 2 \
                                               --executor-memory 4G \
                                               --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar \
                                               complex.py 172.18.1.22 9999 /user/baseline/checkpoint

// 输出如下所示:
服务端: 
nc -lk 9999
wenting
qifeng
wenting
dongshen
streaming输出: 
Counts at time 2016-03-29 10:43:10 [(u'wenting', 2), (u'qifeng', 1), (u'dongshen', 1)]                           
```

#### 4. kafka-Receiver:[kafka Receiver](/src/main/python/streaming/kafka_wordcount.py)
技术点:

    (1) 设置读取kafka数据的量的控制
    (2) 设置kafka读取的偏移量为smallest
    (3) 读取的数据写入kafka中
    (4) 日志回滚设置

代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 2 \
                                               --driver-memory 4G \
                                               --num-executors 2 \
                                               --executor-cores 2 \
                                               --executor-memory 4G \
                                               --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar \
                                               --files log4j-spark.properties \
                                               --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j-spark.properties \
                                               --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j-spark.properties \
                                               kafka_wordcount.py  bgsbtsp0006-dqf:2181,bgsbtsp0007-dqf:2181,bgsbtsp0008-dqf:2181/kafka_0_8_2_1 wenting_spark sparkstreaming /user/baseline/kafka                       
```

#### 4. kafka-Direct:[kafka Direct](/src/main/python/streaming/direct_kafka_wordcount.py)
技术点:
    (1) 设置读取kafka数据的量的控制
    (2) checkpoint 自动保存偏移量
    (3) 打印每次读取的kafka偏移信息
代码提交方式如下:

```
[baseline@bgsbtsp0006-dqf spark]$ spark-submit --master yarn-client \
                                               --driver-cores 2 \
                                               --driver-memory 4G \
                                               --num-executors 2 \
                                               --executor-cores 2 \
                                               --executor-memory 4G \
                                               --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar \
                                               direct_kafka_wordcount.py 172.18.1.22:9092,172.18.1.23:9092,172.18.1.24:9092 wenting_spark
```

### 示例8. MLLib
示例说明见[MLLib demo](/src/main/python/mllib)
