# Spark_SQL_Project

## SparkSql

**SparkSql离线日志分析：使用Scala语言开发**

### 需求分析

1. 网站用户访问时间段分析
2. 网站用户地址分布城市统计分析

### 数据清洗

1. 对不规则数据进行剔除，抽取有用字段存入HDFS文件系统中
2. 使用第三方工具ipdatabase进行ip地址的解析

### 数据分析

使用SparkSql对需求进行分析并且存入关系型数据库中

## SparkWeb

**数据可视化结果展示：使用Java Web开发**

## 需求一结果展示



## 需求二结果展示


## 性能调优

1. 控制文件输出的大小：coalesce
2. 并行度：spark.sql.shuffle.partitions（参数调优）
3. 分区字段类型推测（在指定分区时可以设置为false，默认为true）：spark.sql.sources.partitionColumnTypeInference.enabled（参数调优）


