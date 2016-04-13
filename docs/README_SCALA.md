## Scala 程序示例

### 示例 1: 求 pi 的程序

#### 代码地址为：[SparkPi](/src/main/scala/org/apache/spark/examples/SparkPi.scala)

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.SparkPi \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 512M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 10

// 输出如下所示:
Pi is roughly 3.143388
```

### 示例 2: PageRank 程序

#### 代码地址为：[PageRank](/src/main/scala/org/apache/spark/examples/PageRank.scala)

由于这是一个 PageRank 程序，我们首先需要有一个 links file 用来分析，语料可以从 [stanford snap 网站](http://snap.stanford.edu/data/#web) 下载。

然后我们需要将下载的文件上传到 hdfs 上，比如: /user/qifeng.dai/input/web-Google.txt

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.PageRank \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/web-Google.txt 20

// 输出如下所示(只显示前几条信息):
41909 has rank: 400.44519032034987.
504140 has rank: 375.5010819032476.
597621 has rank: 369.6646236354709.
384666 has rank: 362.6365609156544.
486980 has rank: 351.73446430276226.
537039 has rank: 345.75516945243567.
751384 has rank: 340.5017950696665.
32163 has rank: 339.1166621454229.
765334 has rank: 331.4902795720364.
605856 has rank: 330.52336456651165.
```

### 示例 3: 使用 "广播变量"

先说一下为什么要使用 "广播变量", 在 Spark 中分布式执行的代码需要传递到各个 Executor 的 Task 上运行。对于一些只读的变量，每次都需要 Driver 广播到各个 Task 上，这样效率低下。"广播变量" 即是为了解决这个问题才出现的，它允许程序在每个结点保存一份变量的 cache，而不是在每个 task 里面做 copy，比如可以给每个 node 一个大的 dataset copy 以提高性能。

这种数据的广播方式是先将数据以 serialized 形式进行 cache，然后在每个 task 运行的时候进行 deserialized。

#### 1. 代码地址为: [BroadcastTest](/src/main/scala/org/apache/spark/examples/BroadcastTest.scala)

提交方式为：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.BroadcastTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar dicts.txt /user/qifeng.dai/input/story.txt
```

#### 2. 代码地址为: [BroadcastTest2](/src/main/scala/org/apache/spark/examples/BroadcastTest2.scala), 这个例子展示的是一个自定义 class，读者需要关注其序列化过程

提交方式为：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.BroadcastTest2 \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

注意事项：case class 是带有序列化功能的，如果不是 case class，注意需要继承 Java 序列化接口，或者是采用 KryoSerializer 序列化。

### 示例 4. 使用 "累加器变量"

累加器变量，只提供了 "add" 功能，能被用于实现计数器的功能。如果累加器在创建的时候有名称，在 Spark 的 UI 上能够进行展示出来。需要注意的是只有 driver 能够读取 accumulator 的值。

用户需要注意的地方有：
1. 如果 accumulator 在 actions 里面进行 update，Spark 保证每个 task 对 accumulator 的 update 只发生一次（tasks 重启也不影响重复计算）；
2. 如果在 transformations 里面发生，update 可能发生多次，即在 tasks 或者 job stages re-executed 时发生。

具体的代码地址为：

#### 1. 代码地址为: [AccumulatorTest](/src/main/scala/org/apache/spark/examples/AccumulatorTest.scala)

提交方式为：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.AccumulatorTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

#### 2. 代码地址为: [AccumulatorTest2](/src/main/scala/org/apache/spark/examples/AccumulatorTest2.scala)

提交方式为：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.AccumulatorTest2 \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

// 输出如下所示（注意，结果是随机的）
accum: Map(5009 -> 77, 5003 -> 99, 5006 -> 81, 5005 -> 109, 5008 -> 100, 5002 -> 106, 5010 -> 80, 5004 -> 113, 5007 -> 80, 5001 -> 100)
```

### 示例 5. "读/写" 各种文件

Spark 支持各种数据源，包括 Local-FileSystem，HDFS，HBase，JDBC databases 等，同时也能支持各种文件格式，包括：

文件格式 | 结构化 | 说明
------------ | ------------- | -------------
Text files | No | 普通的 text files，每行代表一个 records
JSON | Semi | 文本格式，半结构化的，大多数的 libraries 都是把每一行当做一个 record
CSV | Yes | 很通用的基于文本的格式
SequenceFiles | Yes | Hadoop 的一种文件格式，表达 key/value 数据的
Protocol buffers | Yes | google 开源的一种文件格式
Object files | Yes | 依赖于 Java Serialization，如果 classes 修改，会失败

#### 1. 读取本地文件: [LocalFileTest](/src/main/scala/org/apache/spark/examples/LocalFileTest.scala)

提交方式:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.LocalFileTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /etc/sysconfig/network
```

#### 2. 读写文本文件的例子(HDFS 上): [HdfsFileTest](/src/main/scala/org/apache/spark/examples/HdfsFileTest.scala)

提交方式:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.HdfsFileTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 2 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /home/qifeng.dai/sparkbook/story.txt /user/qifeng.dai/output/story

[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ hadoop fs -ls /user/qifeng.dai/output/storyFound 5 items
-rw-r--r--   3 qifeng.dai supergroup          0 2016-03-07 16:56 /user/qifeng.dai/output/story/_SUCCESS
-rw-r--r--   3 qifeng.dai supergroup       1662 2016-03-07 16:56 /user/qifeng.dai/output/story/part-00000
-rw-r--r--   3 qifeng.dai supergroup       1105 2016-03-07 16:56 /user/qifeng.dai/output/story/part-00001
-rw-r--r--   3 qifeng.dai supergroup       1268 2016-03-07 16:56 /user/qifeng.dai/output/story/part-00002
-rw-r--r--   3 qifeng.dai supergroup       1143 2016-03-07 16:56 /user/qifeng.dai/output/story/part-00003
```

#### 3. 读写 JSON 文件的例子: [JsonWithJ4s](/src/main/scala/org/apache/spark/examples/JsonWithJ4s.scala)

查看: [Json4S](https://github.com/json4s/json4s)

该代码展示了如何读取一个 JSON 的示例，用到的数据是真实数据，样例如下:

```
{"api_category_name":"UserAction","appkey":"8fda5524dfc4af820f0f68d557381d72","bt":"Chrome46","callback":"BCore.instances[2].callbacks[0]","cb":["C"],"cid":"Cwangfujing","ct":"utf-8","d_s":"pc","ep":"http://172.18.1.22:3033/","fv":"19.0 r0","gid":"87205254007bf9520000031700000001565c2d1c","ip":"172.18.1.22","is_newgid":false,"item_type":"ItemBase","ja":true,"lt":10000,"method":"PageView","oc":"zh-cn","ot":"Windows NT 4.0","p_id":"aa","p_p":"a","p_s":"b","p_t":"hp","ptime":111,"random":"1448881467714","ref_page":"","rs":[1366,768],"sid":"255135432.70309399.1448881462059","terminal":"PC","timestamp":1448933047.0550001,"tma":"255135432.70330821.1448881462063.1448881462063.1448881462063.1","tmc":"2.255135432.70330821.1448881462063.1448881462063.1448881467404","tmd":"2.255135432.70330821.1448881462063.","uid":"255135432.70309399.1448881462059","user_agent":"Apache-HttpClient/4.2.6 (java 1.5)","uuid":"Input:87205254007bf952:0000033d:00037805:565cf6b7"}
```

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ hadoop fs -rmr hdfs://bgsbtsp0006-dqf:8020/user/qifeng.dai/output/result

[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.JsonWithJ4s \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/useraction.json /user/qifeng.dai/output/result
```

处理 JSON 需要注意选择合适的 JSON library，并且正确的配置 dependency(特别注意 scala.binary.version)，另外，scala JSON library 自身的版本也要注意。

#### 4. 读取 CSV 文件的例子: [ParseCsv](/src/main/scala/org/apache/spark/examples/ParseCsv.scala)

CSV 文件格式也是一种比较常见的文件格式，是以 "," 分隔的文件，每一行包含了固定数目的字段，对于 CSV 文件的加载和 JSON 文件格式类似，也有很多 package 支持。

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ hadoop fs -rmr hdfs://bgsbtsp0006-dqf:8020/user/qifeng.dai/output/result

[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ParseCsv \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/useraction.csv /user/qifeng.dai/output/result
```

#### 5. 读取 SequenceFiles 文件的例子: [ParseSequenceFiles](/src/main/scala/org/apache/spark/examples/ParseSequenceFiles.scala)

SequenceFiles 是一种非常流行的 Hadoop format，它包含 key/value pairs，它具有同步标记用来对文件中的记录进行 "切割". 这种特性也使得 Spark 能在多个结点并行处理它。

值得注意的是 SequenceFiles 包含的元素是实现了 Hadoop Writable interface(Hadoop 使用了自己的序列化框架)。大多数的 Hadoop Writable classes 都没有实现 java.io.Serializable，如果要和 RDD 结合使用，需要进行转换。

注意 SparkContext 的 sequenceFile method signature:

```
def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]): RDD[(K, V)]
```

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ParseSequenceFiles \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample.seq
```

#### 6. 读取 Object files 文件的例子: [ParseObject](/src/main/scala/org/apache/spark/examples/ParseObject.scala)

注意，object files 是采用 Java Serialization 来完成的，如果改变了 class 的 fields 信息，那么老的 object file 将无法读取。

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ParseObject \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/objectfile.obj
```

#### 7. 使用 JDBC 接口: [LoadByJDBC](/src/main/scala/org/apache/spark/examples/LoadByJDBC.scala)

Spark 能够访问一些数据库，比如从关系数据库加载数据(使用 JDBC)，包括 MySQL，Postgres 等等。

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.LoadByJDBC \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

#### 8. 操作 HBase: [HBaseTest](/src/main/scala/org/apache/spark/examples/HBaseTest.scala)

Spark 对 HBase 的访问是通过 Hadoop input format 做到的，具体的实现是在 org.apache.hadoop.hbase.mapreduce.TableInputFormat class 中。这个 input format 返回 key/value pairs，key 的类型是 org.apache.hadoop.hbase.io.ImmutableBytesWritable，value 的类型是 org.apache.hadoop.hbase.client.Result。

Result class 包括各种方法获取 values。

具体的接口参加：[SparkContext](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext), [TableInputFormat](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html)

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.HBaseTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar ipplace
```

### 示例6. 使用 --files, --archives

#### 代码地址为: [FilesAndArchivesTest](/src/main/scala/org/apache/spark/examples/FilesAndArchivesTest.scala)

在程序中，我们可能希望将本地的某个文件分发到服务器上，比如词典文件等等，这时候需要用到 --files/--archives。

我们先看看 "--files":

格式为: --files filePath[#sortName]

注意，可以是 --files filePath 的形式，filePath 必须执行本地的某个文件，不能是目录。
也可以是 --files filePath#sortName 的形式，这样在 executor 上面可以直接通过 sortName 来进行引用。

实际上，我们看看 YARN 为 executor 执行准备的环境:

```
--files story.txt
lrwxrwxrwx 1 bfd_hz bfd_hz   61 Mar  9 11:54 story.txt -> /tmp/nm-local-dir/usercache/qifeng.dai/filecache/80/story.txt

--files story.txt#st
lrwxrwxrwx 1 bfd_hz bfd_hz   61 Mar  9 10:41 st -> /tmp/nm-local-dir/usercache/qifeng.dai/filecache/67/story.txt
```

我们再看看 --archives(压缩格式可以为 zip, tar.gz 等):

格式为: --archives archivesPath[#sortName]

注意，可以是 --files archivesPath 的形式，我们可以将目录或者是当个的文件进行打包，打包后的信息会包含目录信息。
也可以是 --files archivesPath#sortName 的形式，与不采用 sortName 的方式一样，只不过是通过 sortName 来进行访问。

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ tar zcvf dicts.tar.gz conf/
conf/
conf/dicts2.txt
conf/dicts.txt

# 我们看看 NodeManager 上的目录信息
[bfd_hz@bgsbtsp0007-dqf container_1456208267763_5750_01_000003]$ ll -R /tmp/nm-local-dir/usercache/qifeng.dai/appcache/application_1456208267763_5781/container_1456208267763_5781_01_000005/z/
/tmp/nm-local-dir/usercache/qifeng.dai/appcache/application_1456208267763_5781/container_1456208267763_5781_01_000005/z/:
total 4
drwx------ 2 bfd_hz bfd_hz 4096 Mar  9 15:03 conf

/tmp/nm-local-dir/usercache/qifeng.dai/appcache/application_1456208267763_5781/container_1456208267763_5781_01_000005/z/conf:
total 8
-r-x------ 1 bfd_hz bfd_hz 211 Mar  9 15:03 dicts2.txt
-r-x------ 1 bfd_hz bfd_hz 152 Mar  9 15:03 dicts.txt

[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.FilesAndArchivesTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 512M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 1024M \
                                        --files story.txt#st \
                                        --archives dicts.tar.gz#z \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar st z blackheads,Adrien
```

### 示例7. 使用 "第三方组件"

#### 代码地址为: [ThirdPartyTest](/src/main/scala/org/apache/spark/examples/ThirdPartyTest.scala)

在实际的场景中，我们可能需要和一些第三方组件通信，比如我们可能想将 "推荐算法" 的运算结果保存在 Redis 中，那么如何处理第三方组件的实例化，如何调用呢？

本例子展示如何正确的使用第三方组件进行开发。

我们用到的测试数据集可从 [movielens](http://grouplens.org/datasets/movielens/) 获取。

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ThirdPartyTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 8 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/ratings.dat 172.18.1.22 6379 0
```

### 示例8. SQL

#### 1 DataFrame 的基本操作: [DataFrameTest](/src/main/scala/org/apache/spark/examples/sql/DataFrameTest.scala)

该示例主要展示 DataFrame 的创建、基本操作、以及 Schema inference 相关的内容，数据以 json 为例。

另外，数据在 resources 目录下有示例，为 weather.txt。

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.sql.DataFrameTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/weather.txt /user/qifeng.dai/output/weather
```

#### 2 数据读写测试: [LoadSaveTest](/src/main/scala/org/apache/spark/examples/sql/LoadSaveTest.scala)

该示例主要展示了数据的读写，包括 table 的写入过程，也对写入的几种模式做了介绍。

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.sql.LoadSaveTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/weather.txt /user/qifeng.dai/output/weather
```

#### 3 JDBC 数据源写入: [SaveJDBCTest](/src/main/scala/org/apache/spark/examples/sql/SaveJDBCTest.scala)

该示例主要展示了写入 mysql 数据表的过程。由于参数较多，这里参数顺序是(url, file-for-write, table-for-write, user, passwd)。

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.sql.SaveJDBCTest \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar jdbc:mysql://172.18.1.22:3306/test /user/qifeng.dai/input/weather.txt weather hive hive123
```

#### 4 JDBC 数据源读取: [LoadJDBCTest](/src/main/scala/org/apache/spark/examples/sql/LoadJDBCTest.scala)

该示例主要展示了读取 mysql 数据表的过程。由于参数较多，这里参数顺序是(url, table-for-read, user, passwd)。

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.sql.LoadJDBCTest \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar jdbc:mysql://172.18.1.22:3306/test weather hive hive123
```

#### 5 Hive 操作: [HiveOperationTest](/src/main/scala/org/apache/spark/examples/sql/HiveOperationTest.scala)

该示例主要展示 HiveContext 的相关使用，包括 udf 相关内容。

注意 --jars, --files。

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.sql.HiveOperationTest \
                                        --jars /home/bfd_hz/spark/lib/datanucleus-api-jdo-3.2.6.jar,/home/bfd_hz/spark/lib/datanucleus-core-3.2.10.jar,/home/bfd_hz/spark/lib/datanucleus-rdbms-3.2.9.jar \
                                        --files /home/bfd_hz/spark/conf/hive-site.xml \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/weather.txt
```

### 示例9. Streaming

本章示例来自 [streaming](https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples/streaming), 这里的示例已经非常之详尽，这里主要对其进行分析讲解，会有少量的改动。

#### 1 Kafka 单词计数 [KafkaWordCount](/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala)

这个例子主要讲解了读取 Kafka 中的数据，然后进行单词计算统计的过程，其中，KafkaWordCountProducer 是用来生成句子放到 kafka 中，KafkaWordCount 则用来统计单词的次数。

首先启动发送单词的 Producer：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ java -classpath ".:$SCALA_HOME/lib/scala-library.jar:./spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar" org.apache.spark.examples.streaming.KafkaWordCountProducer \
                                        172.18.1.22:9092,172.18.1.23:9092,172.18.1.24:9092 test_01 3 5
```

然后我们启动 KafkaWordCount 用来统计一下单词的个数情况：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.streaming.KafkaWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22:2181,172.18.1.23:2181,172.18.1.24:2181/kafka_0_8_2_1 read_01 test_01 2
```

#### 2 Kafka 单词计数-Direct 接口版本 [DirectKafkaWordCount](/src/main/scala/org/apache/spark/examples/streaming/DirectKafkaWordCount.scala)

还是需要启动上面的 Producer：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ java -classpath ".:$SCALA_HOME/lib/scala-library.jar:./spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar" org.apache.spark.examples.streaming.KafkaWordCountProducer \
                                        172.18.1.22:9092,172.18.1.23:9092,172.18.1.24:9092 test_01 3 5
```

然后启动统计程序：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.streaming.DirectKafkaWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22:9092,172.18.1.23:9092,172.18.1.24:9092 test_01
```

#### 3 Hadoop-HDFS 单词计数 [HdfsWordCount](/src/main/scala/org/apache/spark/examples/streaming/HdfsWordCount.scala)

这里主要以某个 HDFS 目录为输入进行统计。

启动方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.streaming.HdfsWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/hdfswordcount
```

可以往这个 hdfs 目录 push 数据，注意要以原子的方式注入。

注: 这个例子测试没有通过

#### 4 RDD Queue 的例子 [QueueStream](/src/main/scala/org/apache/spark/examples/streaming/QueueStream.scala)

这里主要是将 RDD 作为 streaming 来进行读取，通过不断往队列中添加 RDD 来进行分析。

启动方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.streaming.QueueStream \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

#### 5 网络发送单词的计数问题 [SqlNetworkWordCount](/src/main/scala/org/apache/spark/examples/streaming/SqlNetworkWordCount.scala)

启动网络端口，并且往里面发送数据：

```
$ nc -lk 9999

hello world
...
```

启动网络计数的程序：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.streaming.SqlNetworkWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22 9999
```

#### 6 网络发送单词的计数问题 [StatefulNetworkWordCount](/src/main/scala/org/apache/spark/examples/streaming/StatefulNetworkWordCount.scala)

启动网络端口，并且往里面发送数据：

```
$ nc -lk 9999

hello world
...
```

启动网络计数的程序：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.streaming.StatefulNetworkWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22 9999
```

#### 7 具备广播和累加器变量的例子 [RecoverableNetworkWordCount](/src/main/scala/org/apache/spark/examples/streaming/RecoverableNetworkWordCount.scala)

启动网络端口，并且往里面发送数据：

```
$ nc -lk 9999

hello world
...
```

启动网络计数的程序，这个程序会统计丢弃的单词，并且将计数信息写到 Redis 中：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.streaming.RecoverableNetworkWordCount \
                                        --master yarn \
                                        --deploy-mode client \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 172.18.1.22 9999 /user/qifeng.dai/checkpoint 172.18.1.22 6379
```

### 示例10. MLLib

MLlib 是 Spark 的机器学习库，主要包括一些学习算法，如：分类，回归，聚类，协同过滤，维度消减，一些相对 lower-level 的优化功能，以及 higher-level 的 pipeline APIs。

它包括两个 packages:

spark.mllib: 基于 RDDs 的原生的 API；
spark.ml: 提供了 higher-level API，基于 DataFrames，用于构造 ML pipelines。

这里的示例主要介绍 spark.ml，由于官方推荐使用 spark.ml，这里我们也不打算对 spark.mllib 进行介绍。

spark.ml 的相关示例：

=== Overview: estimators, transformers and pipelines ===

#### 1 逻辑回归的例子: [LogisticRegExample](/src/main/scala/org/apache/spark/examples/ml/LogisticRegExample.scala)

该示例主要展示了 Estimator, Transformer, Param 的概念和用法，是一个最基本的应用 spark 构建机器学习样例的例子。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.LogisticRegExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

#### 2 Pipeline 的例子: [PipelineExample](/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala)

该示例展示了简单的 Pipeline 过程，涉及到一个 pipeline 构建过程，模型的保存和加载。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.PipelineExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 4 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

#### 3 交叉验证的例子: [CrossValidationExample](/src/main/scala/org/apache/spark/examples/ml/CrossValidationExample.scala)

该示例展示了一个交叉验证来选择最优模型的例子。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.CrossValidationExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

注，之前配置的 --num-executors 为 4，可能随机性引起一些问题，见 [ArrayIndexOutOfBoundsException](https://mail-archives.apache.org/mod_mbox/incubator-spark-user/201601.mbox/%3CCALo9x1QLNQXfHiV6+tOK+d3n=iPpuOgNet6QEmv4txb+W-okRA@mail.gmail.com%3E)

#### 4 训练集切分验证的例子: [ValidationSplitExample](/src/main/scala/org/apache/spark/examples/ml/ValidationSplitExample.scala)

该示例也是一个通过验证选择最佳模型的例子，不同之处是没有使用交叉验证的方式，而是将数据集一次性按照比例切分为训练集和测试集，能够节省测试时间。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ hadoop fs -put sample_linear_regression_data.txt /user/qifeng.dai/input

[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.ValidationSplitExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_linear_regression_data.txt
```

注：输入的文件格式要符合 libsvm 的格式需求，另外就是第一行数字要是 double 类型的。

=== Extracting, transforming and selecting features ===

#### 5 TfIdf 例子: [TfIdfExample](/src/main/scala/org/apache/spark/examples/ml/TfIdfExample.scala)

TF-IDF 是一种通用的文本处理过程，它分为 TF 和 IDF 两个过程。相关资料请查阅：[tf-idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)

TF 是采用 HashingTF（一种 Transformer）来进行处理，可以得到固定长度的 feature vectors，具体是对每个 word 进行 hash，每个 hash 值对应一个特征（可以设置特征数，hash 值会进行映射）；
IDF 是一种 Estimator，它 fits 一个 dataset，生成一个 IDFModel，这个 IDFModel 接受 feature vectors，然后得到一个扩展的 feature vectors。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.TfIdfExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

#### 6 Word2Vec 例子: [Word2VecExample](/src/main/scala/org/apache/spark/examples/ml/Word2VecExample.scala)

word2vec 模型在文本分析中占有重要地位，具体的资料可以参加：[wiki-word2vec](https://en.wikipedia.org/wiki/Word2vec), 以及 [project-word2vec](http://deeplearning4j.org/word2vec)

Word2Vec 是一个 Estimator，接受一系列的 words（对 documents 的表示），然后训练出一个 Word2VecModel 模型，这个模型对每个 document 会生成一个 vector。然后这个 vector 能被当做特征用于各种 prediction。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.Word2VecExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

#### 7 CountVectorizer 例子: [CountVectorizerExample](/src/main/scala/org/apache/spark/examples/ml/CountVectorizerExample.scala)

CountVectorizer 和 CountVectorizerModel 的目标是将 text 文档集合转化为 token counts 的向量，当没有先验词典，CountVectorizer 当做 estimator 来使用，训练出 CountVectorizerModel，在拟合的过程中，CountVectorizer 会选择 top 的几个 words。可选参数 "minDF" 设置了单个 term 需要在多少个文档中出现的下限制（如果是 <1.0 则为比例）。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.CountVectorizerExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
+--------------------+
|            features|
+--------------------+
|(3,[0,1,2],[1.0,1...|
|(3,[0,1,2],[2.0,2...|
|       (3,[0],[1.0])|
|       (3,[2],[1.0])|
|           (3,[],[])|
+--------------------+
```

#### 8 Tokenizer 的例子，即分词示例: [TokenizerExample](/src/main/scala/org/apache/spark/examples/ml/TokenizerExample.scala)

spark ml 提供了 2 种分词，一种是：Tokenization，另外一种是 RegexTokenizer。

Tokenization 接受一个 text（比如 sentence），然后将其切分为单个的 terms（通常是 words）。

RegexTokenizer 允许更加高级的分词，就是采用正则表达式来完成的。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.TokenizerExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
result of tokenizer...
[WrappedArray(hi, i, heard, about, spark),0]
[WrappedArray(i, wish, java, could, use, case, classes),1]
[WrappedArray(logistic,regression,models,are,neat),2]
result of regex tokenizer...
[WrappedArray(hi, i, heard, about, spark),0]
[WrappedArray(i, wish, java, could, use, case, classes),1]
[WrappedArray(logistic, regression, models, are, neat),2]
```

#### 9 停用词的例子，会删除停用词: [StopWordsRemoverExample](/src/main/scala/org/apache/spark/examples/ml/StopWordsRemoverExample.scala)

停用词指的是应该从 input 中删除的单词，StopWordsRemover 接受 strings 序列，然后从输入中删除停用词。

stopwords 列表由 stopWords 参数指定，默认提供的 stopwords 可以通过 getStopWords 来获取，boolean 参数 caseSensitive 指定是否是大小写敏感的。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.StopWordsRemoverExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
stop words remove
+---+--------------------+--------------------+
| id|                 raw|            filtered|
+---+--------------------+--------------------+
|  0|[I, saw, the, red...|  [saw, red, baloon]|
|  1|[Mary, had, a, li...|[Mary, little, lamb]|
+---+--------------------+--------------------+

stop words remove after add some words
+---+--------------------+--------------------+
| id|                 raw|            filtered|
+---+--------------------+--------------------+
|  0|[I, saw, the, red...|       [saw, baloon]|
|  1|[Mary, had, a, li...|[Mary, little, lamb]|
+---+--------------------+--------------------+
```

#### 10 n-gram 示例: [NGramExample](/src/main/scala/org/apache/spark/examples/ml/NGramExample.scala)

n-gram 是指连续的 n 个 tokens（通常指的是 words），NGram 类用于将输入 features 转化为 n-grams。

参数 n 用于确定 terms 的次数（在每个 n-gram 里面）。输出包含的是一系列的 n-grams。如果我们的输入 sequence 包含的单词小于 n 个 strings，则不会有输出。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.NGramExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
List(Hi I heard, I heard about, heard about Spark)
List(I wish Java, wish Java could, Java could use, could use case, use case classes)
List(Logistic regression models, regression models are, models are neat)
```

#### 11


#### 12


#### 13


#### 14


#### 15


#### 16


=== Classification and regression ===

#### 17


#### 18


#### 19


#### 20


#### 21


#### 22


#### 23


#### 24


#### 25


#### 26


#### 27


#### 28


#### 29


#### 30


#### 31


#### 32


#### 33


#### 34


#### 35


#### 36



=== Clustering ===

#### 37


#### 38


#### 39


#### 40


### 示例11. GraphX

图是一个比较宽泛的主题，关于图以及相关的应用，用户可以参考一些资料，包括 Danai Koutra 教授的 "Node and Graph Similarity: Theory and Applications", Google 的论文 [Pregel](https://www.google.com.sg/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&cad=rja&uact=8&ved=0ahUKEwj-y7Cok8TLAhVLmJQKHe9vDpgQFggdMAA&url=https%3A%2F%2Fkowshik.github.io%2FJPregel%2Fpregel_paper.pdf&usg=AFQjCNFhxY3UjAhPdZIEk51P6ACPOormtA) 等都是非常经典的文献 。

#### 1 图的基本操作: [GraphOperation](/src/main/scala/org/apache/spark/examples/graphx/GraphOperation.scala)

该示例展示了图的一些基本用法，包括图的创建，图的一些基本操作等。

代码提交方式如下:

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.graphx.GraphOperation \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 8 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

#### 2 图的相关算法(PageRank, Connected Components, Triangle Counting): [GraphAlgorithms](/src/main/scala/org/apache/spark/examples/graphx/GraphAlgorithms.scala)

介绍图的基本算法:

PageRank: PageRank 算法用于衡量 graph 中每个 vertex 的重要性，具体可以参见 paper [PageRank](http://infolab.stanford.edu/pub/papers/google.pdf)，GraphX 包括 static 和 dynamic 的 PageRank 实现，static 的 PageRank 运行固定次数的迭代，dynamic 的 PageRank 会直到算法收敛。

Connected Components: 连通图算法对图的每个连通部分进行 label，具体是用连通部分的 lowest-numbered vertex 进行 lable。

Triangle Counting: 三角计算是非常有意思的，它是要解决这种问题，对于一个 vertex，它属于一个 triangle，当且仅当它有 2 个相连的 vertices，且这两个 vertices 有 edge 连接它们。三角计算需要 edges 是 canonical orientation(srcId < dstId)，以及 graph 采用 Graph.partitionBy 的策略。

代码提交方式如下:

```
# pagerank
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.graphx.GraphAlgorithms \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 4096M \
                                        --num-executors 20 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar pagerank /user/qifeng.dai/input/web-Google.txt \
                                        --numEPart=100 --partStrategy=EdgePartition2D --tol=0.01 --output=/user/qifeng.dai/output/pagerank

// or

[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.graphx.GraphAlgorithms \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 4096M \
                                        --num-executors 20 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar pagerank /user/qifeng.dai/input/web-Google.txt \
                                        --numEPart=20 --partStrategy=EdgePartition2D --numIter=50 --output=/user/qifeng.dai/output/pagerank_iter

# Connected Components
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.graphx.GraphAlgorithms \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 4096M \
                                        --num-executors 20 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar cc /user/qifeng.dai/input/web-Google.txt \
                                        --numEPart=20 --partStrategy=EdgePartition2D

# Triangle Counting
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.graphx.GraphAlgorithms \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 4096M \
                                        --num-executors 20 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar triangles /user/qifeng.dai/input/web-Google.txt \
                                        --numEPart=20 --partStrategy=EdgePartition2D
```