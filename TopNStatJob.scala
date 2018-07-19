package cn.just.shinelon.sparkSql_Project.cn.just.shinelon.sparkSql_Project

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val conf =new SparkConf()
          .setAppName("TopNStatJob")
          .setMaster("local[10]")

    val sc=new SparkContext(conf)

    val sqlContext=new SQLContext(sc)

    val tableDF=sqlContext.parquetFile("/user/shinelon/sparkSql/data/page_view_data.parquet")
    //创建临时表
    tableDF.registerTempTable("log")

//    tableDF.printSchema()
//
//    tableDF.show(100)
//    TopNCity(sqlContext,tableDF)

    //按照城市分组统计访问网站的最频繁的时间段
//      TimeTopByCity(sqlContext,tableDF)

    //将访问URL的流量进行统计
      TrafficTopByUrl(sqlContext,tableDF)


  }

  /**
    * 统计访问网站的城市的访问数量
    * @param sqlContext
    * @param tableDF
    */
  def TopNCity(sqlContext: SQLContext,tableDF:DataFrame): Unit ={
//    import sqlContext.implicits._

    //使用DataFrame的方式进行统计
//    val cityTopN=tableDF.filter($"day"==="20150831")
//      .groupBy("city","day")
//      .agg(count("city").as("cityNum"))
//      .orderBy($"cityNum".desc)
//    cityTopN.select(cityTopN("day"),cityTopN("city"),cityTopN("cityNum"))

    //使用SQL的方式进行统计
    val cityTopN=sqlContext.sql("select day,city,count(1) as times from log " +
      "where day='20150831' " +
      "group by city,day " +
      "order by times desc")


    cityTopN.show()

    /**
      * 将统计结果存入Mysql数据库中
      */
    try{
    cityTopN.foreachPartition(cityPartition=>{
      var list=new ListBuffer[CityAccessTopn]
      cityPartition.foreach(lines=>{
        val day=lines.getAs[String](0)
        val city=lines.getAs[String](1)
        val times=lines.getAs[Long](2)

        list.append(CityAccessTopn(day,city,times))
      })
      CityAccessTopnDao.insertCityTopn(list)
    })
    }catch {
      case e:Exception=>e.printStackTrace()
    }
  }

  /**
    * 按照城市分组统计访问网站的最频繁的时间段
    * @param sqlContext
    * @param tableDF
    */
  def TimeTopByCity(sqlContext: SQLContext,tableDF:DataFrame): Unit ={
      val timeTop=sqlContext.sql("select t.hour,count(*) cnt from " +
        "(select substring(time,12,2) hour from log) t " +
        "group by t.hour " +
        "order by cnt desc")

    try{
      timeTop.foreachPartition(timePartition=>{
        var list=new ListBuffer[TimeAccessTopn]
        timePartition.foreach(lines=>{
          val hour=lines.getAs[String](0)
          val cnt=lines.getAs[Long](1)

          list.append(TimeAccessTopn(hour,cnt))
        })
        CityAccessTopnDao.insertTimeTopn(list)
      })
    }catch {
      case e:Exception=>e.printStackTrace()
    }

  }

  /**
    * 将访问URL的流量进行统计并且存入数据库中
    */
  def TrafficTopByUrl(sqlContext: SQLContext,tableDF:DataFrame): Unit ={
    val trafficTop=sqlContext.sql("select day,url,sum(traffic) as traffics from log where day='20150831' " +
      "group by day,url " +
      "order by traffics desc")

//    val jdbcDF=sqlContext.load("jdbc",Map(
//      "url"->"jdbc:mysql://127.0.0.1:3306/sparkSql?user=root&password=123456",
//      "dbtable"->"traffics_topn",
//      "driver"->"com.mysql.jdbc.Driver"
//      //      "user"->"root",
//      //      "password"->"123456"
//    ))
    trafficTop.insertIntoJDBC("jdbc:mysql://127.0.0.1:3306/sparkSql?user=root&password=123456","traffics_topn",true)



//    try{
//      trafficTop.foreachPartition(trafficsPartition=>{
//        var list=new ListBuffer[TrafficAccessTopn]
//        trafficsPartition.foreach(lines=>{
//          val day=lines.getAs[String](0)
//          val url=lines.getAs[String](1)
//          val traffics=lines.getAs[Double](2)
//
//          list.append(TrafficAccessTopn(day,url,traffics))
//        })
//        CityAccessTopnDao.insertTrafficsTopn(list)
//      })
//    }catch {
//      case e:Exception=>e.printStackTrace()
//    }

  }



}
