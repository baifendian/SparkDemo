## ML 程序示例

MLlib 是 Spark 的机器学习库，主要包括一些学习算法，如：分类，回归，聚类，协同过滤，维度消减，一些相对 lower-level 的优化功能，以及 higher-level 的 pipeline APIs。

它包括两个 packages:

spark.mllib: 基于 RDDs 的原生的 API；
spark.ml: 提供了 higher-level API，基于 DataFrames，用于构造 ML pipelines。

这里的示例主要介绍 spark.ml，由于官方推荐使用 spark.ml，这里我们也不打算对 spark.mllib 进行介绍。

spark.ml 的相关示例：

=== Overview: estimators, transformers and pipelines ===

### 1 逻辑回归的例子: [LogisticRegExample](/src/main/scala/org/apache/spark/examples/ml/LogisticRegExample.scala)

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

### 2 Pipeline 的例子: [PipelineExample](/src/main/scala/org/apache/spark/examples/ml/PipelineExample.scala)

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

### 3 交叉验证的例子: [CrossValidationExample](/src/main/scala/org/apache/spark/examples/ml/CrossValidationExample.scala)

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

### 4 训练集切分验证的例子: [ValidationSplitExample](/src/main/scala/org/apache/spark/examples/ml/ValidationSplitExample.scala)

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

=== Extracting, transforming and selecting features(Extracting) ===

### 5 TfIdf 例子: [TfIdfExample](/src/main/scala/org/apache/spark/examples/ml/extraction/TfIdfExample.scala)

TF-IDF 是一种通用的文本处理过程，它分为 TF 和 IDF 两个过程。相关资料请查阅：[tf-idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)

TF 是采用 HashingTF（一种 Transformer）来进行处理，可以得到固定长度的 feature vectors，具体是对每个 word 进行 hash，每个 hash 值对应一个特征（可以设置特征数，hash 值会进行映射）；
IDF 是一种 Estimator，它 fits 一个 dataset，生成一个 IDFModel，这个 IDFModel 接受 feature vectors，然后得到一个扩展的 feature vectors。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.extraction.TfIdfExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

### 6 Word2Vec 例子: [Word2VecExample](/src/main/scala/org/apache/spark/examples/ml/extraction/Word2VecExample.scala)

word2vec 模型在文本分析中占有重要地位，具体的资料可以参加：[wiki-word2vec](https://en.wikipedia.org/wiki/Word2vec), 以及 [project-word2vec](http://deeplearning4j.org/word2vec)

Word2Vec 是一个 Estimator，接受一系列的 words（对 documents 的表示），然后训练出一个 Word2VecModel 模型，这个模型对每个 document 会生成一个 vector。然后这个 vector 能被当做特征用于各种 prediction。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.extraction.Word2VecExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar
```

### 7 CountVectorizer 例子: [CountVectorizerExample](/src/main/scala/org/apache/spark/examples/ml/extraction/CountVectorizerExample.scala)

CountVectorizer 和 CountVectorizerModel 的目标是将 text 文档集合转化为 token counts 的向量，当没有先验词典，CountVectorizer 当做 estimator 来使用，训练出 CountVectorizerModel，在拟合的过程中，CountVectorizer 会选择 top 的几个 words。可选参数 "minDF" 设置了单个 term 需要在多少个文档中出现的下限制（如果是 <1.0 则为比例）。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.extraction.CountVectorizerExample \
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

=== Extracting, transforming and selecting features(transforming) ===

### 8 Tokenizer 的例子，即分词示例: [TokenizerExample](/src/main/scala/org/apache/spark/examples/ml/transformation/TokenizerExample.scala)

spark ml 提供了 2 种分词，一种是：Tokenization，另外一种是 RegexTokenizer。

Tokenization 接受一个 text（比如 sentence），然后将其切分为单个的 terms（通常是 words）。

RegexTokenizer 允许更加高级的分词，就是采用正则表达式来完成的。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.TokenizerExample \
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

### 9 停用词的例子，会删除停用词: [StopWordsRemoverExample](/src/main/scala/org/apache/spark/examples/ml/transformation/StopWordsRemoverExample.scala)

停用词指的是应该从 input 中删除的单词，StopWordsRemover 接受 strings 序列，然后从输入中删除停用词。

stopwords 列表由 stopWords 参数指定，默认提供的 stopwords 可以通过 getStopWords 来获取，boolean 参数 caseSensitive 指定是否是大小写敏感的。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.StopWordsRemoverExample \
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

### 10 n-gram 示例: [NGramExample](/src/main/scala/org/apache/spark/examples/ml/transformation/NGramExample.scala)

n-gram 是指连续的 n 个 tokens（通常指的是 words），NGram 类用于将输入 features 转化为 n-grams。

参数 n 用于确定 terms 的次数（在每个 n-gram 里面）。输出包含的是一系列的 n-grams。如果我们的输入 sequence 包含的单词小于 n 个 strings，则不会有输出。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.NGramExample \
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

### 11 Binarizer 示例: [BinarizerExample](/src/main/scala/org/apache/spark/examples/ml/transformation/BinarizerExample.scala)

Binarization 是对数值类型进行处理，处理为二进制的 (0/1) features.

Binarizer 接受参数 inputCol 和 outputCol，以及一个 threshold 用于做 binarization。大于这个 threshold 的映射为 1.0，小于这个值的映射为 0.0。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.BinarizerExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 0.5

# 结果如下
[0.0]
[1.0]
[0.0]
```

### 12 PCA 示例: [PCAExample](/src/main/scala/org/apache/spark/examples/ml/transformation/PCAExample.scala)

PCA(Principal Component Analysis)是对高维数据进行降维，并且去除噪声的一种数据处理方式，更多资料参考见：[wiki](https://en.wikipedia.org/wiki/Principal_component_analysis)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.PCAExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar 0.5

# 结果如下
+--------------------+
|         pcaFeatures|
+--------------------+
|[1.64857282308838...|
|[-4.6451043317815...|
|[-6.4288805356764...|
+--------------------+
```

### 13 字符串编码示例: [StringIndexerExample](/src/main/scala/org/apache/spark/examples/ml/transformation/StringIndexerExample.scala)

StringIndexer 对 string column 进行编码，编码为 label indices，具体的 indices 是 [0, numLabels)，根据 label 的频次有序排列。

比如，对于：

 id | category
----|----------
 0  | a
 1  | b
 2  | c
 3  | a
 4  | a
 5  | c

我们对 column 中的每个 value 进行处理，会得到如下的处理结果：

 id | category | categoryIndex
----|----------|---------------
 0  | a        | 0.0
 1  | b        | 2.0
 2  | c        | 1.0
 3  | a        | 0.0
 4  | a        | 0.0
 5  | c        | 1.0

这里有些问题就是，对于未见 labels 的处理，目前的方式是：抛出异常(默认策略)或者丢弃。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.StringIndexerExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
+---+--------+-------------+
| id|category|categoryIndex|
+---+--------+-------------+
|  0|       a|          0.0|
|  1|       b|          2.0|
|  2|       c|          1.0|
|  3|       a|          0.0|
|  4|       a|          0.0|
|  5|       c|          1.0|
+---+--------+-------------+
```

### 14 将编码转化为字符串的示例: [IndexToStringExample](/src/main/scala/org/apache/spark/examples/ml/transformation/IndexToStringExample.scala)

这个过程是和上面的 StringIndexer 相反，它将 label 字段还原为 strings，通常的场景是根据 StringIndexer 训练出一个模型，然后从预测的编码中要还原出原始 label 的话，借助 IndexToString。


代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.IndexToStringExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
+---+----------------+
| id|originalCategory|
+---+----------------+
|  0|               a|
|  1|               b|
|  2|               c|
|  3|               a|
|  4|               a|
|  5|               c|
+---+----------------+
```

### 15 向量的编码示例: [VectorIndexerExample](/src/main/scala/org/apache/spark/examples/ml/transformation/VectorIndexerExample.scala)

对 vectors 进行索引编码，会自动的判断出哪个 features 需要编码，哪些不需要，最终会将原始的 values 进行转换，其过程如下：

1. 接受 Vector 和参数 maxCategories.
2. 判断哪些 features 应该被进行编码，这个规则是基于不同的 values，如果不同的 values 最多有 maxCategories 种，则认为应该进行编码.
3. 计算编码，从 0 起始计算.
4. 将原始的 feature values 进行编码.

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.VectorIndexerExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt 10

# 结果如下
Chose 351 categorical features: 645, 69, 365, 138, 101, 479, 333, 249, 0, 555, 666, 88, 170, 115, 276, 308, 5, 449, 120, 247, 614, 677, 202, 10, 56, 533, 142, 500, 340, 670, 174, 42, 417, 24, 37, 25, 257, 389, 52, 14, 504, 110, 587, 619, 196, 559, 638, 20, 421, 46, 93, 284, 228, 448, 57, 78, 29, 475, 164, 591, 646, 253, 106, 121, 84, 480, 147, 280, 61, 221, 396, 89, 133, 116, 1, 507, 312, 74, 307, 452, 6, 248, 60, 117, 678, 529, 85, 201, 220, 366, 534, 102, 334, 28, 38, 561, 392, 70, 424, 192, 21, 137, 165, 33, 92, 229, 252, 197, 361, 65, 97, 665, 583, 285, 224, 650, 615, 9, 53, 169, 593, 141, 610, 420, 109, 256, 225, 339, 77, 193, 669, 476, 642, 637, 590, 679, 96, 393, 647, 173, 13, 41, 503, 134, 73, 105, 2, 508, 311, 558, 674, 530, 586, 618, 166, 32, 34, 148, 45, 161, 279, 64, 689, 17, 149, 584, 562, 176, 423, 191, 22, 44, 59, 118, 281, 27, 641, 71, 391, 12, 445, 54, 313, 611, 144, 49, 335, 86, 672, 172, 113, 681, 219, 419, 81, 230, 362, 451, 76, 7, 39, 649, 98, 616, 477, 367, 535, 103, 140, 621, 91, 66, 251, 668, 198, 108, 278, 223, 394, 306, 135, 563, 226, 3, 505, 80, 167, 35, 473, 675, 589, 162, 531, 680, 255, 648, 112, 617, 194, 145, 48, 557, 690, 63, 640, 18, 282, 95, 310, 50, 67, 199, 673, 16, 585, 502, 338, 643, 31, 336, 613, 11, 72, 175, 446, 612, 143, 43, 250, 231, 450, 99, 363, 556, 87, 203, 671, 688, 104, 368, 588, 40, 304, 26, 258, 390, 55, 114, 171, 139, 418, 23, 8, 75, 119, 58, 667, 478, 536, 82, 620, 447, 36, 168, 146, 30, 51, 190, 19, 422, 564, 305, 107, 4, 136, 506, 79, 195, 474, 664, 532, 94, 283, 395, 332, 528, 644, 47, 15, 163, 200, 68, 62, 277, 691, 501, 90, 111, 254, 227, 337, 122, 83, 309, 560, 639, 676, 222, 592, 364, 100
+-----+--------------------+--------------------+
|label|            features|             indexed|
+-----+--------------------+--------------------+
|  0.0|(692,[127,128,129...|(692,[127,128,129...|
|  1.0|(692,[158,159,160...|(692,[158,159,160...|
|  1.0|(692,[124,125,126...|(692,[124,125,126...|
|  1.0|(692,[152,153,154...|(692,[152,153,154...|
|  1.0|(692,[151,152,153...|(692,[151,152,153...|
|  0.0|(692,[129,130,131...|(692,[129,130,131...|
|  1.0|(692,[158,159,160...|(692,[158,159,160...|
|  1.0|(692,[99,100,101,...|(692,[99,100,101,...|
|  0.0|(692,[154,155,156...|(692,[154,155,156...|
|  0.0|(692,[127,128,129...|(692,[127,128,129...|
|  1.0|(692,[154,155,156...|(692,[154,155,156...|
|  0.0|(692,[153,154,155...|(692,[153,154,155...|
|  0.0|(692,[151,152,153...|(692,[151,152,153...|
|  1.0|(692,[129,130,131...|(692,[129,130,131...|
|  0.0|(692,[154,155,156...|(692,[154,155,156...|
|  1.0|(692,[150,151,152...|(692,[150,151,152...|
|  0.0|(692,[124,125,126...|(692,[124,125,126...|
|  0.0|(692,[152,153,154...|(692,[152,153,154...|
|  1.0|(692,[97,98,99,12...|(692,[97,98,99,12...|
|  1.0|(692,[124,125,126...|(692,[124,125,126...|
+-----+--------------------+--------------------+
only showing top 20 rows
```

### 16 归一化示例: [NormalizerExample](/src/main/scala/org/apache/spark/examples/ml/transformation/NormalizerExample.scala)

归一化是指将每个独立样本做尺度变换从而是该样本具有单位 Lp 范数。这是文本分类和聚类中的常用操作。例如，两个做了 L2 归一化的 TF-IDF 向量的点积是这两个向量的 cosine（余弦）相似度。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.NormalizerExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
+-----+--------------------+--------------------+
|label|            features|        normFeatures|
+-----+--------------------+--------------------+
| -1.0|(119,[2,10,13,18,...|(119,[2,10,13,18,...|
| -1.0|(119,[2,5,16,26,3...|(119,[2,5,16,26,3...|
| -1.0|(119,[3,5,14,20,3...|(119,[3,5,14,20,3...|
| -1.0|(119,[4,5,14,21,3...|(119,[4,5,14,21,3...|
| -1.0|(119,[1,5,15,21,3...|(119,[1,5,15,21,3...|
| -1.0|(119,[1,5,13,19,3...|(119,[1,5,13,19,3...|
| -1.0|(119,[0,5,13,21,3...|(119,[0,5,13,21,3...|
| -1.0|(119,[0,5,16,18,3...|(119,[0,5,16,18,3...|
| -1.0|(119,[1,5,17,19,3...|(119,[1,5,17,19,3...|
|  1.0|(119,[4,10,14,31,...|(119,[4,10,14,31,...|
| -1.0|(119,[4,15,29,34,...|(119,[4,15,29,34,...|
| -1.0|(119,[4,5,14,19,3...|(119,[4,5,14,19,3...|
| -1.0|(119,[4,6,15,28,3...|(119,[4,6,15,28,3...|
| -1.0|(119,[0,10,17,19,...|(119,[0,10,17,19,...|
|  1.0|(119,[4,17,18,38,...|(119,[4,17,18,38,...|
| -1.0|(119,[1,10,17,19,...|(119,[1,10,17,19,...|
| -1.0|(119,[0,5,16,21,3...|(119,[0,5,16,21,3...|
| -1.0|(119,[0,5,13,19,3...|(119,[0,5,13,19,3...|
| -1.0|(119,[3,5,17,21,3...|(119,[3,5,17,21,3...|
| -1.0|(119,[1,5,14,18,3...|(119,[1,5,14,18,3...|
+-----+--------------------+--------------------+
only showing top 20 rows

+-----+--------------------+--------------------+
|label|            features|        normFeatures|
+-----+--------------------+--------------------+
| -1.0|(119,[2,10,13,18,...|(119,[2,10,13,18,...|
| -1.0|(119,[2,5,16,26,3...|(119,[2,5,16,26,3...|
| -1.0|(119,[3,5,14,20,3...|(119,[3,5,14,20,3...|
| -1.0|(119,[4,5,14,21,3...|(119,[4,5,14,21,3...|
| -1.0|(119,[1,5,15,21,3...|(119,[1,5,15,21,3...|
| -1.0|(119,[1,5,13,19,3...|(119,[1,5,13,19,3...|
| -1.0|(119,[0,5,13,21,3...|(119,[0,5,13,21,3...|
| -1.0|(119,[0,5,16,18,3...|(119,[0,5,16,18,3...|
| -1.0|(119,[1,5,17,19,3...|(119,[1,5,17,19,3...|
|  1.0|(119,[4,10,14,31,...|(119,[4,10,14,31,...|
| -1.0|(119,[4,15,29,34,...|(119,[4,15,29,34,...|
| -1.0|(119,[4,5,14,19,3...|(119,[4,5,14,19,3...|
| -1.0|(119,[4,6,15,28,3...|(119,[4,6,15,28,3...|
| -1.0|(119,[0,10,17,19,...|(119,[0,10,17,19,...|
|  1.0|(119,[4,17,18,38,...|(119,[4,17,18,38,...|
| -1.0|(119,[1,10,17,19,...|(119,[1,10,17,19,...|
| -1.0|(119,[0,5,16,21,3...|(119,[0,5,16,21,3...|
| -1.0|(119,[0,5,13,19,3...|(119,[0,5,13,19,3...|
| -1.0|(119,[3,5,17,21,3...|(119,[3,5,17,21,3...|
| -1.0|(119,[1,5,14,18,3...|(119,[1,5,14,18,3...|
+-----+--------------------+--------------------+
only showing top 20 rows
```

### 17 向量 "相乘" 的示例 [ElementwiseProductExample](/src/main/scala/org/apache/spark/examples/ml/transformation/ElementwiseProductExample.scala)

对每个输入的向量，需要乘上一个 "weight" 向量，这个乘法也是相当的简单，计算规则见：[Hadamard product](https://en.wikipedia.org/wiki/Hadamard_product_%28matrices%29)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.ElementwiseProductExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
+---+-------------+-----------------+
| id|       vector|transformedVector|
+---+-------------+-----------------+
|  a|[1.0,2.0,3.0]|    [0.0,2.0,6.0]|
|  b|[4.0,5.0,6.0]|   [0.0,5.0,12.0]|
+---+-------------+-----------------+
```

### 18 SQL 语句进行转换的示例: [SQLTransformerExample](/src/main/scala/org/apache/spark/examples/ml/transformation/SQLTransformerExample.scala)

支持通过写 SQL 语句来完成 transformations，这个确实很强大，不过只支持一些简单的语法，如 "SELECT ... FROM __THIS__ ...". __THIS__ 表示输入 dataset 的名称。

select 语句设置了具体的输出，包括 fields，常量，表达式，比如有：

1. SELECT a, a + b AS a_b FROM __THIS__
2. SELECT a, SQRT(b) AS b_sqrt FROM __THIS__ where a > 5
3. SELECT a, b, SUM(c) AS c_sum FROM __THIS__ GROUP BY a, b

假设我们的数据是：

 id |  v1 |  v2
----|-----|-----
 0  | 1.0 | 3.0
 2  | 2.0 | 5.0

我们的语句是 "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__", 那么结果是：

 id |  v1 |  v2 |  v3 |  v4
----|-----|-----|-----|-----
 0  | 1.0 | 3.0 | 4.0 | 3.0
 2  | 2.0 | 5.0 | 7.0 |10.0

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.SQLTransformerExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
+---+---+---+---+----+
| id| v1| v2| v3|  v4|
+---+---+---+---+----+
|  0|1.0|3.0|4.0| 3.0|
|  2|2.0|5.0|7.0|10.0|
+---+---+---+---+----+
```

### 19 向量的合并操作 [VectorAssemblerExample](/src/main/scala/org/apache/spark/examples/ml/transformation/VectorAssembler.scala)

向量合并的工作是需要将给定的 column list 合并为一个唯一的 vector column，一般用于将 raw features 和由其它 transformers 转化的 features 进行合并，得到一个单个的 feature vector。

一般为了训练 ML models(比如 logistic regression 和 decision tree)是需要做这个事情的。VectorAssembler 接受的 column types 包括 numeric types，boolean type，vector type 等等。

在每一行，input columns 的值会进行连接(根据特定顺序).

比如对于下面的 DataFrame:

 id | hour | mobile | userFeatures     | clicked
----|------|--------|------------------|---------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0

希望将 hour, mobile 和 userFeatures 进行结合得到一个 feature vector，转化后会得到：

 id | hour | mobile | userFeatures     | clicked | features
----|------|--------|------------------|---------|-----------------------------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0     | [18.0, 1.0, 0.0, 10.0, 0.5]

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.transformation.VectorAssemblerExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
[[18.0,1.0,0.0,10.0,0.5],1.0]
```

=== Extracting, transforming and selecting features(selecting features) ===

### 20 向量切片转换器示例: [VectorSlicerExample](/src/main/scala/org/apache/spark/examples/ml/selection/VectorSlicerExample.scala)

VectorSlicer 是一个 transformer，它接受一个 feature vector，输出的是一个新的 feature vector，它具备原始 features 的 sub-array。一般用于从 vector column 中抽取特征。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.selection.VectorSlicerExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
[[-2.0,2.3,0.0],[2.3,0.0]]
```

### 21 卡方分布获取特征示例: [ChiSqSelectorExample](/src/main/scala/org/apache/spark/examples/ml/selection/ChiSqSelectorExample.scala)

根据 [Chi-Squared](https://en.wikipedia.org/wiki/Chi-squared) 的方式来选取特征，首先会根据 Chi-Squared test 来对 features 进行排序（其实会检验 features 和 label 之间的独立性），然后过滤选出最靠前的几个特征，最终得出的 features 是最有预测能力的。

比如我们有如下的 DataFrame:

id | features              | clicked
---|-----------------------|---------
 7 | [0.0, 0.0, 18.0, 1.0] | 1.0
 8 | [0.0, 1.0, 12.0, 0.0] | 0.0
 9 | [1.0, 0.0, 15.0, 0.1] | 0.0

然后我们设置 numTopFeatures = 1，根据我们的 clicked label 选择出来的 features 为：

id | features              | clicked | selectedFeatures
---|-----------------------|---------|------------------
 7 | [0.0, 0.0, 18.0, 1.0] | 1.0     | [1.0]
 8 | [0.0, 1.0, 12.0, 0.0] | 0.0     | [0.0]
 9 | [1.0, 0.0, 15.0, 0.1] | 0.0     | [0.1]

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.selection.ChiSqSelectorExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 1 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下
+---+------------------+-------+----------------+
| id|          features|clicked|selectedFeatures|
+---+------------------+-------+----------------+
|  7|[0.0,0.0,18.0,1.0]|    1.0|           [1.0]|
|  8|[0.0,1.0,12.0,0.0]|    0.0|           [0.0]|
|  9|[1.0,0.0,15.0,0.1]|    0.0|           [0.1]|
+---+------------------+-------+----------------+
```

=== Classification ===

### 22 逻辑回归示例: [LogisticRegressionExample](/src/main/scala/org/apache/spark/examples/ml/classification/LogisticRegressionExample.scala)

逻辑回归是一种比较流行的二分类问题，具体可以参见一些资料: [Logistic_regression](https://en.wikipedia.org/wiki/Logistic_regression)

目前在 ml 中，只支持了二分类，未来会对多分类问题也进行支持。

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.classification.LogisticRegressionExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
Coefficients: (692,[244,263,272,300,301,328,350,351,378,379,405,406,407,428,433,434,455,456,461,462,483,484,489,490,496,511,512,517,539,540,568],[-7.353983524188262E-5,-9.102738505589527E-5,-1.9467430546904626E-4,-2.0300642473487015E-4,-3.1476183314860777E-5,-6.842977602660796E-5,1.5883626898245863E-5,1.402349709137544E-5,3.5432047524968383E-4,1.1443272898171381E-4,1.0016712383667109E-4,6.01410930379546E-4,2.8402481791227455E-4,-1.1541084736508898E-4,3.8599688631290186E-4,6.350195574241048E-4,-1.150641238457574E-4,-1.5271865864986873E-4,2.804933808994195E-4,6.070117471191623E-4,-2.0084596632474595E-4,-1.4210755792901355E-4,2.7390103411608675E-4,2.773045624496799E-4,-9.838027027269408E-5,-3.808522443517932E-4,-2.5315198008556074E-4,2.774771477075416E-4,-2.4436197639192967E-4,-0.001539474468759761,-2.3073328411332247E-4]) Intercept: 0.2245631596125049
0.6833149135741656
0.6662875751473731
0.6217068546034619
0.6127265245887888
0.606034798680287
0.6031750687571562
0.5969621534836276
0.5940743031983124
0.5906089243339021
0.5894724576491039
0.588218777572959
+---+--------------------+
|FPR|                 TPR|
+---+--------------------+
|0.0|                 0.0|
|0.0|0.017543859649122806|
|0.0| 0.03508771929824561|
|0.0| 0.05263157894736842|
|0.0| 0.07017543859649122|
|0.0| 0.08771929824561403|
|0.0| 0.10526315789473684|
|0.0| 0.12280701754385964|
|0.0| 0.14035087719298245|
|0.0| 0.15789473684210525|
|0.0| 0.17543859649122806|
|0.0| 0.19298245614035087|
|0.0| 0.21052631578947367|
|0.0| 0.22807017543859648|
|0.0| 0.24561403508771928|
|0.0|  0.2631578947368421|
|0.0|  0.2807017543859649|
|0.0|  0.2982456140350877|
|0.0|  0.3157894736842105|
|0.0|  0.3333333333333333|
+---+--------------------+
only showing top 20 rows

1.0
bestThreshold: 0.5585022394278361
(0.0) -> prob=[0.676482724316062,0.32351727568393795], prediction=0.0
(1.0) -> prob=[0.22640965216205305,0.7735903478379469], prediction=1.0
(1.0) -> prob=[0.2210316383828498,0.7789683616171502], prediction=1.0
(1.0) -> prob=[0.2526490765347192,0.7473509234652809], prediction=1.0
(1.0) -> prob=[0.22494007343582248,0.7750599265641775], prediction=1.0
(0.0) -> prob=[0.6766450451466386,0.32335495485336146], prediction=0.0
(1.0) -> prob=[0.22928932070495942,0.7707106792950406], prediction=1.0
(1.0) -> prob=[0.441497760572164,0.5585022394278361], prediction=0.0
(0.0) -> prob=[0.5258703919180372,0.4741296080819628], prediction=0.0
(0.0) -> prob=[0.6730857354540937,0.3269142645459063], prediction=0.0
(1.0) -> prob=[0.21675509297450063,0.7832449070254994], prediction=1.0
(0.0) -> prob=[0.6433037415078707,0.3566962584921292], prediction=0.0
(0.0) -> prob=[0.6887773785344743,0.3112226214655257], prediction=0.0
(1.0) -> prob=[0.24290074953354387,0.7570992504664562], prediction=1.0
......
```

### 23 决策树分类示例: [DecisionTreeClassificationExample](/src/main/scala/org/apache/spark/examples/ml/classification/DecisionTreeClassificationExample.scala)

决策树也是一种非常流行的分类算法，具体可以参见一些资料: [Decision_tree_learning](https://en.wikipedia.org/wiki/Decision_tree_learning)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.classification.DecisionTreeClassificationExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
+--------------+-----+--------------------+
|predictedLabel|label|            features|
+--------------+-----+--------------------+
|           1.0| -1.0|(119,[2,10,13,18,...|
|          -1.0| -1.0|(119,[1,5,13,19,3...|
|          -1.0| -1.0|(119,[0,5,13,21,3...|
|          -1.0| -1.0|(119,[0,5,16,18,3...|
|          -1.0|  1.0|(119,[4,17,18,38,...|
+--------------+-----+--------------------+
only showing top 5 rows

Test Error = 0.20425531914893613
Learned classification tree model:
DecisionTreeClassificationModel (uid=dtc_e4886039aa5a) of depth 5 with 57 nodes
  If (feature 39 in {0.0})
   If (feature 73 in {0.0})
    If (feature 38 in {0.0})
     If (feature 5 in {1.0})
      If (feature 80 in {0.0})
       Predict: 0.0
      Else (feature 80 not in {0.0})
       Predict: 1.0
     Else (feature 5 not in {1.0})
      If (feature 43 in {1.0})
       Predict: 0.0
      Else (feature 43 not in {1.0})
       Predict: 1.0
    Else (feature 38 not in {0.0})
     If (feature 13 in {1.0})
      If (feature 2 in {0.0})
       Predict: 0.0
      Else (feature 2 not in {0.0})
       Predict: 1.0
     Else (feature 13 not in {1.0})
      Predict: 1.0
   Else (feature 73 not in {0.0})
    If (feature 38 in {0.0})
     If (feature 98 in {0.0})
      If (feature 50 in {0.0})
       Predict: 0.0
      Else (feature 50 not in {0.0})
       Predict: 0.0
     Else (feature 98 not in {0.0})
      Predict: 1.0
    Else (feature 38 not in {0.0})
     If (feature 81 in {0.0})
      If (feature 31 in {0.0})
       Predict: 0.0
      Else (feature 31 not in {0.0})
       Predict: 1.0
     Else (feature 81 not in {0.0})
      If (feature 28 in {0.0})
       Predict: 0.0
      Else (feature 28 not in {0.0})
       Predict: 1.0
  Else (feature 39 not in {0.0})
   If (feature 38 in {0.0})
    If (feature 50 in {0.0})
     If (feature 3 in {0.0})
      If (feature 4 in {0.0})
       Predict: 0.0
      Else (feature 4 not in {0.0})
       Predict: 0.0
     Else (feature 3 not in {0.0})
      If (feature 34 in {1.0})
       Predict: 0.0
      Else (feature 34 not in {1.0})
       Predict: 1.0
    Else (feature 50 not in {0.0})
     If (feature 73 in {1.0})
      If (feature 16 in {0.0})
       Predict: 0.0
      Else (feature 16 not in {0.0})
       Predict: 1.0
     Else (feature 73 not in {1.0})
      Predict: 1.0
   Else (feature 38 not in {0.0})
    If (feature 18 in {0.0})
     If (feature 15 in {0.0})
      If (feature 108 in {1.0})
       Predict: 0.0
      Else (feature 108 not in {1.0})
       Predict: 1.0
     Else (feature 15 not in {0.0})
      If (feature 77 in {1.0})
       Predict: 0.0
      Else (feature 77 not in {1.0})
       Predict: 1.0
    Else (feature 18 not in {0.0})
     If (feature 73 in {0.0})
      If (feature 53 in {1.0})
       Predict: 0.0
      Else (feature 53 not in {1.0})
       Predict: 1.0
     Else (feature 73 not in {0.0})
      If (feature 75 in {1.0})
       Predict: 0.0
      Else (feature 75 not in {1.0})
       Predict: 1.0
```

### 24 随机森林示例: [RandomForestClassifierExample](/src/main/scala/org/apache/spark/examples/ml/classification/RandomForestClassifierExample.scala)

随机森林应该说是一种非常 popular 的分类和回归方法，具体可以参见一些资料: [Random_forest](https://en.wikipedia.org/wiki/Random_forest)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.classification.RandomForestClassifierExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
+--------------+-----+--------------------+
|predictedLabel|label|            features|
+--------------+-----+--------------------+
|           1.0|  1.0|(692,[158,159,160...|
|           1.0|  1.0|(692,[152,153,154...|
|           0.0|  0.0|(692,[154,155,156...|
|           0.0|  0.0|(692,[151,152,153...|
|           1.0|  1.0|(692,[129,130,131...|
+--------------+-----+--------------------+
only showing top 5 rows

Test Error = 0.0
Learned classification forest model:
RandomForestClassificationModel (uid=rfc_1b92a2b3d9fe) with 10 trees
  Tree 0 (weight 1.0):
    If (feature 385 <= 0.0)
     If (feature 206 <= 0.0)
      If (feature 360 <= 0.0)
       Predict: 0.0
      Else (feature 360 > 0.0)
       Predict: 1.0
     Else (feature 206 > 0.0)
      Predict: 1.0
    Else (feature 385 > 0.0)
     Predict: 1.0
  Tree 1 (weight 1.0):
    If (feature 462 <= 0.0)
     If (feature 429 <= 0.0)
      If (feature 296 <= 0.0)
       Predict: 1.0
      Else (feature 296 > 0.0)
       Predict: 0.0
     Else (feature 429 > 0.0)
      Predict: 1.0
    Else (feature 462 > 0.0)
     Predict: 0.0
  Tree 2 (weight 1.0):
    If (feature 512 <= 0.0)
     Predict: 0.0
    Else (feature 512 > 0.0)
     Predict: 1.0
  Tree 3 (weight 1.0):
    If (feature 512 <= 0.0)
     If (feature 523 <= 31.0)
      Predict: 0.0
     Else (feature 523 > 31.0)
      If (feature 578 <= 151.0)
       Predict: 1.0
      Else (feature 578 > 151.0)
       Predict: 0.0
    Else (feature 512 > 0.0)
     Predict: 1.0
  Tree 4 (weight 1.0):
    If (feature 462 <= 0.0)
     If (feature 240 <= 253.0)
      Predict: 1.0
     Else (feature 240 > 253.0)
      Predict: 0.0
    Else (feature 462 > 0.0)
     Predict: 0.0
  Tree 5 (weight 1.0):
    If (feature 429 <= 0.0)
     If (feature 462 <= 0.0)
      Predict: 1.0
     Else (feature 462 > 0.0)
      Predict: 0.0
    Else (feature 429 > 0.0)
     Predict: 1.0
  Tree 6 (weight 1.0):
    If (feature 518 <= 0.0)
     If (feature 405 <= 103.0)
      Predict: 1.0
     Else (feature 405 > 103.0)
      Predict: 0.0
    Else (feature 518 > 0.0)
     If (feature 156 <= 244.0)
      If (feature 489 <= 0.0)
       Predict: 1.0
      Else (feature 489 > 0.0)
       Predict: 0.0
     Else (feature 156 > 244.0)
      Predict: 1.0
  Tree 7 (weight 1.0):
    If (feature 540 <= 65.0)
     If (feature 510 <= 0.0)
      Predict: 0.0
     Else (feature 510 > 0.0)
      Predict: 1.0
    Else (feature 540 > 65.0)
     Predict: 1.0
  Tree 8 (weight 1.0):
    If (feature 463 <= 0.0)
     If (feature 598 <= 0.0)
      If (feature 346 <= 0.0)
       Predict: 0.0
      Else (feature 346 > 0.0)
       Predict: 1.0
     Else (feature 598 > 0.0)
      Predict: 1.0
    Else (feature 463 > 0.0)
     Predict: 0.0
  Tree 9 (weight 1.0):
    If (feature 510 <= 0.0)
     If (feature 517 <= 0.0)
      If (feature 490 <= 0.0)
       Predict: 1.0
      Else (feature 490 > 0.0)
       Predict: 0.0
     Else (feature 517 > 0.0)
      Predict: 0.0
    Else (feature 510 > 0.0)
     Predict: 1.0
```

### 25 Gradient-boosted 树分类示例: [GradientBoostedTreeClassifierExample](/src/main/scala/org/apache/spark/examples/ml/classification/GradientBoostedTreeClassifierExample.scala)

Gradient-boosted trees(GBTs) 是一种非常 popular 的分类和回归方法，具体可以参见一些资料: [Gradient_boosting](https://en.wikipedia.org/wiki/Gradient_boosting)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.classification.GradientBoostedTreeClassifierExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
+--------------+-----+--------------------+
|predictedLabel|label|            features|
+--------------+-----+--------------------+
|           1.0|  1.0|(692,[158,159,160...|
|           1.0|  1.0|(692,[124,125,126...|
|           1.0|  1.0|(692,[151,152,153...|
|           1.0|  0.0|(692,[129,130,131...|
|           1.0|  1.0|(692,[158,159,160...|
+--------------+-----+--------------------+
only showing top 5 rows

Test Error = 0.08108108108108103
Learned classification GBT model:
GBTClassificationModel (uid=gbtc_6a4d82be3417) with 10 trees
  Tree 0 (weight 1.0):
    If (feature 351 <= 15.0)
     If (feature 350 <= 225.0)
      Predict: 1.0
     Else (feature 350 > 225.0)
      Predict: -1.0
    Else (feature 351 > 15.0)
     Predict: -1.0
  Tree 1 (weight 0.1):
    If (feature 434 <= 0.0)
     If (feature 379 <= 0.0)
      Predict: 0.47681168808847024
     Else (feature 379 > 0.0)
      Predict: -0.4768116880884694
    Else (feature 434 > 0.0)
     Predict: -0.4768116880884701
  Tree 2 (weight 0.1):
    If (feature 434 <= 0.0)
     If (feature 379 <= 0.0)
      Predict: 0.4381935810427206
     Else (feature 379 > 0.0)
      Predict: -0.43819358104271977
    Else (feature 434 > 0.0)
     If (feature 212 <= 0.0)
      If (feature 123 <= 29.0)
       Predict: -0.4381935810427206
      Else (feature 123 > 29.0)
       Predict: -0.4381935810427205
     Else (feature 212 > 0.0)
      Predict: -0.4381935810427207
  Tree 3 (weight 0.1):
    If (feature 490 <= 31.0)
     If (feature 323 <= 198.0)
      If (feature 602 <= 0.0)
       If (feature 155 <= 0.0)
        Predict: 0.4051496802845983
       Else (feature 155 > 0.0)
        Predict: 0.4051496802845984
      Else (feature 602 > 0.0)
       Predict: 0.4051496802845983
     Else (feature 323 > 198.0)
      Predict: -0.4051496802845982
    Else (feature 490 > 31.0)
     Predict: -0.4051496802845983
  Tree 4 (weight 0.1):
    If (feature 490 <= 31.0)
     If (feature 100 <= 165.0)
      If (feature 235 <= 37.0)
       If (feature 183 <= 0.0)
        Predict: 0.3765841318352991
       Else (feature 183 > 0.0)
        If (feature 183 <= 86.0)
         Predict: 0.3765841318352991
        Else (feature 183 > 86.0)
         Predict: 0.37658413183529915
      Else (feature 235 > 37.0)
       Predict: 0.3765841318352993
     Else (feature 100 > 165.0)
      Predict: -0.3765841318352994
    Else (feature 490 > 31.0)
     Predict: -0.3765841318352992
  Tree 5 (weight 0.1):
    If (feature 407 <= 0.0)
     If (feature 568 <= 0.0)
      Predict: -0.35166478958101005
     Else (feature 568 > 0.0)
      If (feature 209 <= 141.0)
       If (feature 157 <= 86.0)
        Predict: 0.35166478958101005
       Else (feature 157 > 86.0)
        Predict: 0.35166478958101005
      Else (feature 209 > 141.0)
       Predict: 0.3516647895810101
    Else (feature 407 > 0.0)
     Predict: -0.35166478958100994
  Tree 6 (weight 0.1):
    If (feature 407 <= 0.0)
     If (feature 568 <= 0.0)
      Predict: -0.32974984655529926
     Else (feature 568 > 0.0)
      If (feature 579 <= 27.0)
       Predict: 0.32974984655529926
      Else (feature 579 > 27.0)
       Predict: 0.3297498465552993
    Else (feature 407 > 0.0)
     If (feature 460 <= 24.0)
      Predict: -0.32974984655529926
     Else (feature 460 > 24.0)
      Predict: -0.3297498465552993
  Tree 7 (weight 0.1):
    If (feature 434 <= 0.0)
     If (feature 549 <= 253.0)
      If (feature 133 in {1.0})
       Predict: 0.3103372455197956
      Else (feature 133 not in {1.0})
       Predict: 0.31033724551979563
     Else (feature 549 > 253.0)
      Predict: -0.31033724551979525
    Else (feature 434 > 0.0)
     If (feature 407 <= 0.0)
      Predict: -0.3103372455197956
     Else (feature 407 > 0.0)
      Predict: -0.31033724551979563
  Tree 8 (weight 0.1):
    If (feature 434 <= 0.0)
     If (feature 568 <= 253.0)
      If (feature 155 <= 0.0)
       Predict: 0.2930291649125433
      Else (feature 155 > 0.0)
       If (feature 156 <= 242.0)
        If (feature 95 in {0.0})
         Predict: 0.2930291649125433
        Else (feature 95 not in {0.0})
         Predict: 0.2930291649125434
       Else (feature 156 > 242.0)
        Predict: 0.2930291649125434
     Else (feature 568 > 253.0)
      Predict: -0.29302916491254294
    Else (feature 434 > 0.0)
     If (feature 378 <= 110.0)
      Predict: -0.2930291649125433
     Else (feature 378 > 110.0)
      If (feature 379 <= 168.0)
       If (feature 97 in {0.0})
        Predict: -0.2930291649125433
       Else (feature 97 not in {0.0})
        Predict: -0.2930291649125434
      Else (feature 379 > 168.0)
       Predict: -0.2930291649125434
  Tree 9 (weight 0.1):
    If (feature 462 <= 0.0)
     If (feature 268 <= 253.0)
      If (feature 133 in {1.0})
       Predict: 0.27750666438358246
      Else (feature 133 not in {1.0})
       Predict: 0.27750666438358257
     Else (feature 268 > 253.0)
      Predict: -0.27750666438358174
    Else (feature 462 > 0.0)
     If (feature 239 <= 0.0)
      Predict: -0.27750666438358246
     Else (feature 239 > 0.0)
      Predict: -0.27750666438358257
```

### 26 多层感知机分类算法的示例: [MultilayerPerceptronClassifierExample](/src/main/scala/org/apache/spark/examples/ml/classification/MultilayerPerceptronClassifierExample.scala)

多层感知机分类算法是一种神经网络的算法，这种算法提出的时间应该算是非常久了，具体可以参见一些资料: [mlp](http://deeplearning.net/tutorial/mlp.html)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.classification.MultilayerPerceptronClassifierExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_multiclass_classification_data.txt

# 结果如下
Precision:0.9444444444444444
```

### 27 One-vs-Rest 分类器示例: [OneVsRestExample](/src/main/scala/org/apache/spark/examples/ml/classification/OneVsRestExample.scala)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.classification.OneVsRestExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar \
                                       --input /user/qifeng.dai/input/sample_libsvm_data.txt --fracTest 0.4

# 结果如下
 Training Time 12 sec

 Prediction Time 0 sec

 Confusion Matrix
 21.0  0.0
0.0   20.0

label	fpr
0	0.0
1	0.0
```

=== Regression ===

### 28 线性回归示例: [LinearRegressionWithElasticNetExample](/src/main/scala/org/apache/spark/examples/ml/regression/LinearRegressionWithElasticNetExample.scala)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.regression.LinearRegressionWithElasticNetExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
Coefficients: (692,[300,378,406,407,433,434,461,462,483,489,490,517],[-1.8078924738535276E-6,2.0278116139676124E-4,3.023979903864483E-4,1.4914792601448672E-4,2.1023616090368408E-4,3.2238027474542236E-4,1.4705998776106348E-4,3.043294522421017E-4,-4.4086910361962904E-7,1.4268258742227365E-4,1.4373333249357234E-4,1.44520875664922E-4]) Intercept: 0.29988458600501944
numIterations: 11
objectiveHistory: List(0.4949999999999999, 0.46399875031817656, 0.4336824247251912, 0.42624784802433213, 0.41495322169597887, 0.40518718310849106, 0.40189116243195355, 0.39727355840898415, 0.3948017818428975, 0.39108137561895845, 0.3904422784616588)
+--------------------+
|           residuals|
+--------------------+
|-0.29931609019502237|
|  0.2076176352543707|
| 0.19244665926887083|
|  0.2746457891280665|
|  0.1961473470132742|
| -0.3066202686593571|
| 0.21291588692079233|
|  0.6470733926246883|
| -0.3198689956910858|
| -0.2993764892622182|
| 0.17927483015846524|
|-0.29931564932591875|
|-0.29931564932591875|
| 0.25639586496389644|
| -0.3904771873162662|
| 0.18975466813223396|
| -0.2997849495876014|
| -0.2994189459443439|
| 0.20784522758355228|
| 0.21786989912981214|
+--------------------+
only showing top 20 rows

RMSE: 0.26965884993334
r2: 0.7033215204105606
```

### 29 决策树回归示例: [DecisionTreeRegressionExample](/src/main/scala/org/apache/spark/examples/ml/regression/DecisionTreeRegressionExample.scala)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.regression.DecisionTreeRegressionExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
+----------+-----+--------------------+
|prediction|label|            features|
+----------+-----+--------------------+
|       0.0|  0.0|(692,[127,128,129...|
|       0.0|  0.0|(692,[153,154,155...|
|       0.0|  0.0|(692,[151,152,153...|
|       1.0|  1.0|(692,[150,151,152...|
|       1.0|  1.0|(692,[97,98,99,12...|
+----------+-----+--------------------+
only showing top 5 rows

Root Mean Squared Error (RMSE) on test data = 0.0
Learned regression tree model:
DecisionTreeRegressionModel (uid=dtr_82e4e2d5d6ed) of depth 2 with 5 nodes
  If (feature 434 <= 0.0)
   If (feature 99 in {0.0,3.0})
    Predict: 0.0
   Else (feature 99 not in {0.0,3.0})
    Predict: 1.0
  Else (feature 434 > 0.0)
   Predict: 1.0
```

### 30 随机森林回归示例: [RandomForestRegressorExample](/src/main/scala/org/apache/spark/examples/ml/regression/RandomForestRegressorExample.scala)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.regression.RandomForestRegressorExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
+----------+-----+--------------------+
|prediction|label|            features|
+----------+-----+--------------------+
|       1.0|  1.0|(692,[158,159,160...|
|       1.0|  1.0|(692,[152,153,154...|
|       1.0|  1.0|(692,[151,152,153...|
|      0.45|  0.0|(692,[154,155,156...|
|       0.0|  0.0|(692,[127,128,129...|
+----------+-----+--------------------+
only showing top 5 rows

Root Mean Squared Error (RMSE) on test data = 0.1559709045845509
Learned regression forest model:
RandomForestRegressionModel (uid=rfr_30593cad8b99) with 20 trees
  Tree 0 (weight 1.0):
    If (feature 490 <= 0.0)
     Predict: 0.0
    Else (feature 490 > 0.0)
     Predict: 1.0
  Tree 1 (weight 1.0):
    If (feature 357 <= 0.0)
     Predict: 1.0
    Else (feature 357 > 0.0)
     Predict: 0.0
  Tree 2 (weight 1.0):
    If (feature 462 <= 0.0)
     Predict: 0.0
    Else (feature 462 > 0.0)
     Predict: 1.0
  Tree 3 (weight 1.0):
    If (feature 406 <= 0.0)
     Predict: 0.0
    Else (feature 406 > 0.0)
     If (feature 294 <= 254.0)
      Predict: 1.0
     Else (feature 294 > 254.0)
      Predict: 0.0
  Tree 4 (weight 1.0):
    If (feature 434 <= 0.0)
     If (feature 627 <= 0.0)
      Predict: 1.0
     Else (feature 627 > 0.0)
      Predict: 0.0
    Else (feature 434 > 0.0)
     Predict: 1.0
  Tree 5 (weight 1.0):
    If (feature 400 <= 0.0)
     If (feature 483 <= 0.0)
      Predict: 1.0
     Else (feature 483 > 0.0)
      Predict: 0.0
    Else (feature 400 > 0.0)
     Predict: 0.0
  Tree 6 (weight 1.0):
    If (feature 456 <= 0.0)
     Predict: 1.0
    Else (feature 456 > 0.0)
     Predict: 0.0
  Tree 7 (weight 1.0):
    If (feature 511 <= 0.0)
     Predict: 1.0
    Else (feature 511 > 0.0)
     Predict: 0.0
  Tree 8 (weight 1.0):
    If (feature 406 <= 0.0)
     Predict: 0.0
    Else (feature 406 > 0.0)
     Predict: 1.0
  Tree 9 (weight 1.0):
    If (feature 406 <= 0.0)
     Predict: 0.0
    Else (feature 406 > 0.0)
     If (feature 400 <= 0.0)
      Predict: 1.0
     Else (feature 400 > 0.0)
      Predict: 0.0
  Tree 10 (weight 1.0):
    If (feature 455 <= 23.0)
     Predict: 1.0
    Else (feature 455 > 23.0)
     Predict: 0.0
  Tree 11 (weight 1.0):
    If (feature 407 <= 0.0)
     If (feature 597 <= 0.0)
      Predict: 1.0
     Else (feature 597 > 0.0)
      Predict: 0.0
    Else (feature 407 > 0.0)
     Predict: 1.0
  Tree 12 (weight 1.0):
    If (feature 483 <= 0.0)
     If (feature 524 <= 180.0)
      Predict: 1.0
     Else (feature 524 > 180.0)
      Predict: 0.0
    Else (feature 483 > 0.0)
     Predict: 0.0
  Tree 13 (weight 1.0):
    If (feature 490 <= 0.0)
     Predict: 0.0
    Else (feature 490 > 0.0)
     Predict: 1.0
  Tree 14 (weight 1.0):
    If (feature 379 <= 0.0)
     Predict: 0.0
    Else (feature 379 > 0.0)
     Predict: 1.0
  Tree 15 (weight 1.0):
    If (feature 483 <= 0.0)
     If (feature 659 <= 251.0)
      Predict: 1.0
     Else (feature 659 > 251.0)
      Predict: 0.0
    Else (feature 483 > 0.0)
     Predict: 0.0
  Tree 16 (weight 1.0):
    If (feature 462 <= 0.0)
     If (feature 407 <= 71.0)
      Predict: 0.0
     Else (feature 407 > 71.0)
      Predict: 1.0
    Else (feature 462 > 0.0)
     Predict: 1.0
  Tree 17 (weight 1.0):
    If (feature 517 <= 41.0)
     Predict: 0.0
    Else (feature 517 > 41.0)
     Predict: 1.0
  Tree 18 (weight 1.0):
    If (feature 511 <= 0.0)
     Predict: 1.0
    Else (feature 511 > 0.0)
     Predict: 0.0
  Tree 19 (weight 1.0):
    If (feature 434 <= 0.0)
     Predict: 0.0
    Else (feature 434 > 0.0)
     Predict: 1.0
```

### 31 Gradient-boosted 树回归示例: [GradientBoostedTreeRegressorExample](/src/main/scala/org/apache/spark/examples/ml/regression/GradientBoostedTreeRegressorExample.scala)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.regression.GradientBoostedTreeRegressorExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
+----------+-----+--------------------+
|prediction|label|            features|
+----------+-----+--------------------+
|       1.0|  1.0|(692,[151,152,153...|
|       0.0|  0.0|(692,[153,154,155...|
|       1.0|  1.0|(692,[129,130,131...|
|       0.0|  0.0|(692,[153,154,155...|
|       0.0|  0.0|(692,[155,156,180...|
+----------+-----+--------------------+
only showing top 5 rows

Root Mean Squared Error (RMSE) on test data = 0.21821789023599236
Learned regression GBT model:
GBTRegressionModel (uid=gbtr_08a2dc9b9618) with 10 trees
  Tree 0 (weight 1.0):
    If (feature 406 <= 72.0)
     If (feature 99 in {0.0,3.0})
      Predict: 0.0
     Else (feature 99 not in {0.0,3.0})
      Predict: 1.0
    Else (feature 406 > 72.0)
     Predict: 1.0
  Tree 1 (weight 0.1):
    Predict: 0.0
  Tree 2 (weight 0.1):
    Predict: 0.0
  Tree 3 (weight 0.1):
    Predict: 0.0
  Tree 4 (weight 0.1):
    Predict: 0.0
  Tree 5 (weight 0.1):
    Predict: 0.0
  Tree 6 (weight 0.1):
    Predict: 0.0
  Tree 7 (weight 0.1):
    Predict: 0.0
  Tree 8 (weight 0.1):
    Predict: 0.0
  Tree 9 (weight 0.1):
    Predict: 0.0
```

### 32 Survival 回归示例: [AFTSurvivalRegressionExample](/src/main/scala/org/apache/spark/examples/ml/regression/AFTSurvivalRegressionExample.scala)

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.regression.AFTSurvivalRegressionExample \
                                       --master yarn \
                                       --deploy-mode cluster \
                                       --driver-cores 1 \
                                       --driver-memory 1024M \
                                       --num-executors 4 \
                                       --executor-cores 2 \
                                       --executor-memory 4096M \
                                       spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_libsvm_data.txt

# 结果如下
Coefficients: [0.0,0.0] Intercept: 0.0 Scale: 1.0
+-----+------+--------------+----------+--------------------------------------+
|label|censor|features      |prediction|quantiles                             |
+-----+------+--------------+----------+--------------------------------------+
|1.218|1.0   |[1.56,-0.605] |1.0       |[0.3566749439387325,0.916290731874155]|
|2.949|0.0   |[0.346,2.158] |1.0       |[0.3566749439387325,0.916290731874155]|
|3.627|0.0   |[1.38,0.231]  |1.0       |[0.3566749439387325,0.916290731874155]|
|0.273|1.0   |[0.52,1.151]  |1.0       |[0.3566749439387325,0.916290731874155]|
|4.199|0.0   |[0.795,-0.226]|1.0       |[0.3566749439387325,0.916290731874155]|
+-----+------+--------------+----------+--------------------------------------+
```

=== Clustering ===

### 33 KMeans 聚类示例: [KMeansExample](/src/main/scala/org/apache/spark/examples/ml/cluster/KMeansExample.scala)

k-means 是一种最常用的聚类算法，将数据聚类到指定数目的簇中，MLLib 实现的称之为 kmeans。

kmenas 实现为 Estimator，会生成一个 KMeansModel 作为基础模型。

输入字段：

Param name	| Type(s)	| Default	| Description
------------ | ------------- | ------------- | -------------
featuresCol	|Vector	|"features"	| Feature vector

输出字段：

Param name	| Type(s)	| Default	| Description
------------ | ------------- | ------------- | -------------
predictionCol	| Int	| "prediction"	| Predicted cluster center

代码提交方式如下：

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.KMeansExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar

# 结果如下：
Final Centers:
[0.1,0.1,0.1]
[9.1,9.1,9.1]
Show cluster results:
+---+-------------+----------+
| id|     features|prediction|
+---+-------------+----------+
|  1|[0.0,0.0,0.0]|         0|
|  2|[0.1,0.1,0.1]|         0|
|  3|[0.2,0.2,0.2]|         0|
|  4|[9.0,9.0,9.0]|         1|
|  5|[9.1,9.1,9.1]|         1|
|  6|[9.2,9.2,9.2]|         1|
+---+-------------+----------+
```

### 34 LDA 示例: [LDAExample](/src/main/scala/org/apache/spark/examples/ml/cluster/LDAExample.scala)

LDA 实现为一个 Estimator，支持 EMLDAOptimizer， OnlineLDAOptimizer，且生成了一个 LDAModel 作为基本模型。

```
[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ hadoop fs -put sample_lda_data.txt /user/qifeng.dai/input

[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.ml.LDAExample \
                                        --master yarn \
                                        --deploy-mode cluster \
                                        --driver-cores 1 \
                                        --driver-memory 1024M \
                                        --num-executors 1 \
                                        --executor-cores 2 \
                                        --executor-memory 4096M \
                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/sample_lda_data.txt

# 结果如下：
+-----+-----------+---------------------------------------------------------------+
|topic|termIndices|termWeights                                                    |
+-----+-----------+---------------------------------------------------------------+
|0    |[2, 5, 7]  |[0.10611209730255416, 0.10574766403724752, 0.10434600433804693]|
|1    |[1, 6, 2]  |[0.10188529395467784, 0.09819200099100663, 0.09634168813701709]|
|2    |[1, 9, 4]  |[0.1952783191983112, 0.17326278578442453, 0.10181929606557996] |
|3    |[0, 4, 8]  |[0.10274239793791831, 0.09845109851178913, 0.09817827809411327]|
|4    |[9, 6, 4]  |[0.10457230278625598, 0.10419019333385023, 0.10107149024069599]|
|5    |[1, 10, 0] |[0.10218423494861882, 0.10132240816023727, 0.09514945439624999]|
|6    |[3, 10, 9] |[0.2382177510843298, 0.10794622714690916, 0.10005947666364023] |
|7    |[4, 0, 2]  |[0.1086094478753975, 0.10338136184733017, 0.10037887910975259] |
|8    |[0, 7, 8]  |[0.11014028724241325, 0.09922333731846354, 0.09813155808923131]|
|9    |[9, 6, 8]  |[0.10109365303695003, 0.10016220768323436, 0.09771430186361449]|
+-----+-----------+---------------------------------------------------------------+

-------------------------------------------------------------------------------------------------------------------------------------+
|features                                     |topicDistribution                                                                                                                                                                                                      |
+---------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|[1.0,2.0,6.0,0.0,2.0,3.0,1.0,1.0,0.0,0.0,3.0]|[0.559036435980724,0.004883537047295509,0.401510611865048,0.004883520684053514,0.0048834774868766525,0.005078627088032499,0.005073267559495126,0.004883580192337014,0.004883458906096287,0.004883483190041482]         |
|[1.0,3.0,0.0,1.0,3.0,0.0,0.0,2.0,0.0,0.0,1.0]|[0.008144244528834069,0.008143964257497128,0.9260593143087349,0.008144265369901637,0.008143884890625494,0.008469699226862217,0.00846212961306118,0.008144192194735311,0.008144279645322562,0.008144025964425657]       |
|[1.0,4.0,1.0,0.0,0.0,4.0,9.0,0.0,1.0,2.0,0.0]|[0.004246126581046358,0.00424622753114809,0.9614500269066649,0.004246171663056825,0.004246166744122798,0.004415752196876764,0.004411323366669456,0.004246067243740519,0.0042459676803535355,0.004246170086320767]      |
|[2.0,1.0,0.0,3.0,0.0,0.0,5.0,0.0,2.0,3.0,9.0]|[0.003755634118406685,0.00375570099114229,0.0037560126042513586,0.0037557423516802455,0.003755759871297694,0.0039057400139752576,0.9660482530624831,0.003755672006534214,0.003755750089411688,0.0037557348908173237]   |
|[3.0,1.0,1.0,9.0,3.0,0.0,2.0,0.0,0.0,1.0,3.0]|[0.004068817576370612,0.004068851520768028,0.004069395734983568,0.004068903183451257,0.004068929256372615,0.00423135232604123,0.9632171738605811,0.004068911265175646,0.004068886383748583,0.004068778892507256]       |
|[4.0,2.0,0.0,3.0,4.0,5.0,1.0,1.0,1.0,4.0,0.0]|[0.0037558215128704647,0.0037558002773460278,0.479104990261175,0.0037558819608317228,0.00375580605502679,0.0039057727229171004,0.4906985159949397,0.0037558598866956586,0.0037558167546120516,0.003755734573585476]    |
|[2.0,1.0,0.0,3.0,0.0,0.0,5.0,0.0,2.0,2.0,9.0]|[0.0039059576461011186,0.0039060353273486247,0.003906567074883087,0.003906080804574632,0.00390608962556911,0.004062083718189656,0.9646890370425734,0.00390600011942111,0.0039060837612781966,0.0039060648800610166]    |
|[1.0,1.0,1.0,9.0,2.0,1.0,2.0,0.0,0.0,1.0,3.0]|[0.004438928516347127,0.004438944556088742,0.004439326997618251,0.0044389622635411545,0.004438995425948088,0.004616172472866551,0.9598718882064289,0.004438955000891348,0.004438929306834869,0.004438897253434951]     |
|[4.0,4.0,0.0,3.0,4.0,2.0,1.0,3.0,0.0,0.0,0.0]|[0.004439335764047067,0.004439279608162629,0.5225036911213273,0.004439407673598101,0.004439233605924027,0.004616679764683396,0.44180440936355864,0.0044393857491182315,0.004439359824219863,0.004439217525360935]      |
|[2.0,8.0,2.0,0.0,3.0,0.0,2.0,0.0,2.0,7.0,2.0]|[0.0033667732779825233,0.0033667985196006093,0.9694338299187785,0.003366797233603742,0.0033668266041936732,0.0035011723519084575,0.003497406995361678,0.003366804553862522,0.0033667973586059318,0.0033667931861024107]|
|[1.0,1.0,1.0,9.0,0.0,2.0,2.0,0.0,0.0,3.0,3.0]|[0.004245726062686813,0.00424574097643037,0.004246398721723638,0.004245720320374733,0.004245759294506533,0.00441520884275636,0.9616182944942443,0.004245695249585071,0.004245716210027093,0.004245739827665096]        |
|[4.0,1.0,0.0,0.0,4.0,5.0,1.0,3.0,0.0,1.0,0.0]|[0.004883584448476539,0.004883515376787107,0.0048836957142951495,0.9556634928588742,0.004883488803784198,0.005078566185072326,0.005073067729107334,0.004883642822122609,0.00488352567913721,0.004883420382343438]      |
+---------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```