package cn.just.shinelon.sparkSql_Project

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对日志文件进行数据清洗
  */
object SparkDataFormatJob {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkDataFormatJob")

    val sc=new SparkContext(conf)
    //从HDFS文件系统上读取数据
    val textRDD=sc.textFile("/user/shinelon/sparkSql/pre_data/page_view.log")

    val dataRDD=textRDD.map(text=>{
      val data=text.split(" ")
      val ip=data(0).replace("\"","")
      val time=data(2)+" "+data(3)
      val traffic=data(8).replace("\"","")      //耗费的流量
      var url=data(10).replace("\"","")         //访问的URL地址
      val log_data=ip+"\t"+TimeDateFormatUtil.parseDate(time)+"\t"+url+"\t"+traffic
      log_data
      //将清洗完的数据存入HDFS文件系统中
    }).saveAsTextFile("/user/shinelon/sparkSql/data/page_view_data.log")

  }



}
