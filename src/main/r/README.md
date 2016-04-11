## SparkR示例

### 示例程序说明
#### demo_in_out.R	
数据导入导出示例程序，用于说明如何将数据加载为DataFrame以及如何将DataFrame导出到文件中
#### demo_df_operation.R	
DataFrame操作示例程序，用于说明如何对DataFrame进行常用操作（包括相关SQL操作）。	
#### demo_ml_glm.R	
机器学习GLM方法示例程序，用于说明如何在R中使用分布式机器学习方法
#### demo_wordcount.R
RDD API使用示例程序，因为新版SparkR隐藏了RDD操作相关函数，该示例介绍了如何使用RDD的常用transform和action。
#### demo_package.R
外包使用示例程序。该示例说明如何处理在spark executor上使用R packages的情况。


### 程序的提交
* 通过SparkR
运行模式通过--master或在sparkR.init指定，如下
```
sparkR --master yarn-client demo_wordcount.R /user/bre/xiang.xu/en_article
```
* 通过spark-submit
运行模式通过--master或在sparkR.init指定，如下
```
spark-submit --master yarn-client demo_wordcount.R /user/bre/xiang.xu/en_article
```
提交的资源设置参数与其他语言相同

### RStudio中使用SparkR
待补充

### 注意事项
* Spark版本的注意
因为SparkR正在开发中，不同版本的API存在差别，新版本会增加新的方法
	* 原有RDD相关的action和transform方法(如parallelize, reduce)被隐藏，使用DataFrame
	* 1.4.0之前不支持ml相关方法
	* 1.5.2之前不支持binomial GLM
	* 随着版本更新，示例程序也会及时更新和增加

该程序主要用于介绍使用方法，如果发现问题、有疑问的地方或者有更好的方法都可以QQ或者Email(xiang.xu AT baifendian.com)联系我，谢谢。
