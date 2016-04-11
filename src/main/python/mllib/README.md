## Python MLLib 代码示例

### 示例1：协同过滤-ALS
这个算法比较适用于推荐系统中，它虽然也是协同过滤，但是基于物品和基于用户的方式不同，它
通过挖掘出隐含的因素来预测用户的喜欢的物品，比如用户喜欢的是偏男性还是偏女性的物品，物
品是偏娱乐还是偏严肃等都是隐含因素。通过ALS算法优化可以分别得到用户和物品的隐含因素矩阵
，然后通过这两个矩阵进行预测。它相对于其他的协同过滤算法相对准确率和可拓展性较好。

#### 代码地址：Collaborative_filtering目录下
#### 数据：使用的数据是movielens的数据集：movies，在data目录下

将它放到hdfs中：/user/movies.dat
                /user/ratings.dat
提交代码：
```
    spark-submit Collaborative_filtering.py ratings.dat movies.dat predict_id
```
预测id为4时的部分结果如下：
```
 1: Love & Sex (2000)
 2: Excalibur (1981)
 3: 13th Warrior, The (1999)
 4: Evil Dead, The (1981)
 5: Brother from Another Planet, The (1984)
 6: Jonah Who Will Be 25 in the Year 2000 (Jonas qui aura 25 ans en l'an 2000) (1976)
 7: Random Hearts (1999)
 8: Snow Falling on Cedars (1999)
 9: Howl's Moving Castle (Hauru no ugoku shiro) (2004)
10: Castle in the Sky (Tenk no shiro Rapyuta) (1986)
11: We Were Soldiers (2002)
12: Me, Myself and Irene (2000)
13: Dirty Rotten Scoundrels (1988)
14: Tales from the Crypt Presents: Demon Knight (1995)
15: Blood Simple (1984)
......
```

### 示例2：关联规则-FP-Growth
FP-Growth是Apriori算法的一种改进，在生成频繁项集的时候Apriori是搜索产生所有的可能项集，然后进行筛选，使用
的是生成-测试的方法，时间复杂度高。而FP-Growth使用类似Trie树的数据结构，叫做FP-tree，可以压缩数据，并在这
棵树上获取频繁项集，复杂度较低.
#### 代码地址：FPGroup目录下
#### 数据：使用的数据集是超市购物车的数据，可以在这里下载：[data](),用到的是excel中sheet1的数据，
使用pretreatment.py预处理并将数据放到hdfs上。如：/user/dataset_operator.txt

#### 提交代码：
```
    spark-submit --executor-memory 1024M --executor-cores 2 FPGrow.py dataset_operator.txt
```
部分结果如下：
```
冲饮食品
冲饮食品 酱油
罐头食品 饮料
滋补保健品 乳制冲饮
冲饮食品 罐头食品
冲饮食品 醋
冲饮食品 滋补保健品
乳制冲饮 调味品
冲饮食品 中式挂面/通心粉
冲饮食品 即食主食
......
```

### 示例3：LDA主题模型
Latent Dirichlet Allocation是无监督算法，通过对文档-单词共现关系建模，得到文档在隐含变量即主题以及单词在主题变量上的分布。示例包含三个代码文件：
1. demo_segment.py  分词和去除停用词示例代码
2. demo_vectorize.py   文本向量化示例代码（使用HashingTF方法）
3. demo_lda.py   LDA训练模型（v1.6.1中LDA新文档预测的Python API接口还未有，所以示例中只有训练）

说明：该示例需要1.6.0版本以上
#### 代码地址：LDA目录下
#### 数据：
搜狗新闻数据集，选取三个类目抽样得到数据样例，data目录下的sogou_news_text。停用词可以使用data目录下的stopword，注意的是停用词文件需要使用--files参数将文件分发到节点上。
#### 提交代码：
```
    spark-submit --master yarn-client --files stopword --py-files demo_segment.py,demo_vectorize.py demo_lda.py /user/xiang.xu/demo_data /user/xiang.xu/model_lda stopword 3 lda
```

### 示例4: 线性分类器-线性SVM
#### 代码地址为：Linear_classifly/LinearSVM.py和LinearSVM_predict.py
目前pyspark中SVM只实现了线性分类器，不能使用核函数，损失函数是hinge loss.  

#### 数据：使用的数据是UCI上的adult数据集，可以在这里下载:[data](http://archive.ics.uci.edu/ml/datasets/Adult)，需要用到的是训练代码adult.data和测试数据adult_test.data，可以使用pretreament.py对数据进行预处理，然后将处理完成后的数据放到hdfs上。比如：
```
    /user/adult_operator.data
    /user/adult_test_operator.data
```
然后提交代码：
```
    spark-submit --executor-memory 1024M --executor-cores 2 --master yarn-client LinearSVM.py adult_operator.data
```
代码会将模型保存到hafs上，地址为/user/Model.  
最后提交测试函数：
```
    spark-submit --executor-memory 1024M --executor-cores 2 LinearSVM_predict.py adult_test_operator.data
```
得到的输出如下：
```
    Test Error = 0.0
```

### 示例5：线性分类器-logistics回归
#### 代码地址为；Linear_classifly/LinearRegress.py和LinearRegress_predict.py
目前pyspark中logistics还不支持多分类。使用的数据集可以是上面的adult，将数据集放在hdfs上后提交
代码：
```
    spark-submit --executor-memory 1024M --executor-cores 2 --master yarn-client LogisticRegress.py adult_operator.data
```
代码将模型保存到hdfs中：/user/Model_logistc  
提交测试代码：
```
    spark-submit --executor-memory 1024M --executor-cores 2 LogisticRegress_predict.py adult_test_operator.data
```
得到的输出如下:
```
    Test Error = 0.78
```

### 示例6：线性分类器-线性回归
#### 代码地址为：Linear_classifly/LinearRegress.py和LinearRegress_predict.py
线性回归是指使用最小二乘法拟合原始数据，数据的标签可以是连续的。  
这里还是使用adult数据集，提交训练代码：
```
    spark-submit --executor-memory 1024M --executor-cores 2 --master yarn-client LinearRegress.py adult_operator.data
```
执行完了之后会将模型存放到hdfs中：/user/Model_Linear  
提交测试代码：
```
    spark-submit --executor-memory 1024M --executor-cores 2 LinearRegress_predict.py adult_test_operator.data
```
得到的输出如下：
```
    Test Error = 1.0
```

### 示例7：ml pipeline的使用
#### 代码地址：ml_pipeline目录下
#### 数据：搜狗新闻数据集部分数据，在data目录下的sogou_news_text。
该pipeline demo包含以下步骤：
1. labelIndexer 将标签索引，从字符转化为整数
2. tokenizer 将句子分成单词
3. remover 去除停用词
4. word2vec 使用word2vec将文本转化为低维度向量
5. mpc 神经网络分类器

说明：该示例需要1.6.0版本以上
#### 提交代码如下：
```
    spark-submit --master local[2] demo_pipeline.py /user/xiang.xu/test /user/xiang.xu/demo_model stopword 3 pipeline
```
#### 得到结果如下：
```
    Precision: 0.735849056604
```

### 示例8：ml pipeline的使用2
#### 代码地址：ml_pipeline2目录下
#### 数据：adult数据集
该pipeline demo包含以下步骤：
1. PCA 对adult中的features进行降维
2. LogisiticRegress 对降维好的数据进行分类

#### 提交代码如下：
```
    spark-submit pipline.py adult_operator.data
```

#### 得到结果如下：
```
    Row(label=1.0, features=DenseVector([30.0, 77516.0, 13.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0,  
    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 
    0.0, 1.0, 2174.0, 0.0, 40.0]), prediction=0.0)
```