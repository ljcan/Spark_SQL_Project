package cn.just.shinelon.sparkSql_Project.cn.just.shinelon.sparkSql_Project

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext, sql}

/**
  * 完成日志文件的解析清洗操作
  */
object log {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setMaster("local[2]")
          .setAppName("log")

    val sc=new SparkContext(conf)

    val accessRDD=sc.textFile("/user/shinelon/sparkSql/data/page_view_data.log")

    //创建sqlContext
    val sqlContext=new SQLContext(sc)

    //RDD=>DF
    val accessDF= sqlContext.createDataFrame(accessRDD.map(x=>{
        AccessConvertUtil2.parseLog(x)
    }),AccessConvertUtil2.structType)

//    accessDF.printSchema()
    //
    //    accessDF.show(1000)

    SaveMode.Overwrite

    /**
      * saveAsParquetFile源码：
      * def saveAsParquetFile(path: String): Unit = {
        if (sqlContext.conf.parquetUseDataSourceApi) {
          save("org.apache.spark.sql.parquet", SaveMode.ErrorIfExists, Map("path" -> path))
        } else {
          sqlContext.executePlan(WriteToFile(path, logicalPlan)).toRdd
        }
  }
      */
//    accessDF.save("org.apache.spark.sql.parquet",SaveMode.Overwrite,
//      Map(("path","/user/shinelon/sparkSql/data/page_view_data.parquet")))
    accessDF.registerTempTable("page_view_table")
//    sqlContext.sql("select day,count(*) from page_view_table day")
    accessDF.saveAsParquetFile("/user/shinelon/sparkSql/data/page_view_data.parquet")


    sc.stop()
  }

}
