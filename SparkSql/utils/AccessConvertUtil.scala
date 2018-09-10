package cn.just.shinelon.sparkSql_Project.cn.just.shinelon.sparkSql_Project

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object AccessConvertUtil {

  val structType=StructType{
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),      //课程类型
      StructField("cmsId",StringType),          //课程ID
      StructField("traffic",StringType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  }

  /**
    * 将每一行的输入信息转换为输出信息
    * @param accessLog
    * @return
    */
  def parseLog(accessLog:String) ={

    try{
    val splits=accessLog.split("\t")

    val url=splits(2)
    val ip=splits(0)
    val traffic=splits(3)

    var cmsType=""
    var cmsId=""
//    val domain1="http://www.ibeifeng.com/"
    val domain="http://learn.ibeifeng.com/"

    //http://learn.ibeifeng.com/course/view.php?id=42&p=3
    if(url.length>1){
      if(url.contains(domain)){
        val urlSplits=url.substring(domain.length).split("/")
        if(urlSplits!=""&&urlSplits.length<=2){
          cmsType=urlSplits(0)
          if(urlSplits(1).contains("&"))
            cmsId=urlSplits(1).substring(urlSplits(1).indexOf("id=")+1,urlSplits(1).indexOf("&")+1)
          else
            cmsId=urlSplits(1).substring(urlSplits(1).indexOf("id=")+1,urlSplits(1).length)
        }else if(urlSplits!=""&&urlSplits.length>2){
          cmsType=urlSplits(0)
          val cms= String.valueOf(urlSplits.toBuffer.remove(0))        //将数组转换为字符串
          if(cms.contains("&"))
            cmsId=cms.substring(cms.indexOf("id=")+1,cms.indexOf("&")+1)
          else
            cmsId=cms.substring(cms.indexOf("id=")+1,cms.length)
        }
      }
    }
    val city=""
    val time=splits(1)
    val day=time.substring(0,10).replace("-","")      //截取天信息

    Row(url,cmsType,cmsId,traffic,ip,city,time,day)

    }catch {
      case e:Exception=>{
//        throw e
        println("解析异常")
        Row(0)
      }
    }

  }

  def main(args: Array[String]): Unit = {
    val a="http://just.learn.com/course/view.php?id=42&p=3"

    parseLog(a)
  }

}
