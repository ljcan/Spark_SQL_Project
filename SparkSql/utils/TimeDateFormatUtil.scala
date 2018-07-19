package cn.just.shinelon.sparkSql_Project



import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat




/**
  * 日期格式转换工具
  */
object TimeDateFormatUtil {

    //31/Aug/2015:00:04:37+0800
  //原始日期格式
  //SimpleDateFormat线程不安全，不能使用
//  val OLDER_DATE_FORMAT=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  val OLDER_DATE_FORMAT=FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

  //目标日期格式
  val TARGET_DATE_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  //解析日期
  def parseDate(time: String):String={
    TARGET_DATE_FORMAT.format(new Date(getTimes(time)))
  }

  //输入日期的格式：31/Aug/2015:00:04:37+0800
  def getTimes(time:String):Long={
    try {
      val date=time.substring(time.indexOf("\"")+1, time.lastIndexOf("\""))
      OLDER_DATE_FORMAT.parse(date).getTime
    }catch {
      case e:Exception=>{
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val l=parseDate("\"31/Aug/2015:00:04:37 +0800\"")
    println(l)
  }

}
