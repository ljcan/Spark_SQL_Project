package cn.just.shinelon.sparkSql_Project.cn.just.shinelon.sparkSql_Project






import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 将访问的城市信息存入数据的DAO类
  */
object CityAccessTopnDao {

  var connection:Connection=null
  var pstm:PreparedStatement=null

  def insertCityTopn(list:ListBuffer[CityAccessTopn]): Unit ={
    try{
      connection=MysqlUtil.getConnection()
      connection.setAutoCommit(false)       //设置为手动提交
      val  sql="insert into city_topn(day,city,time) values(?,?,?)"
      pstm=connection.prepareStatement(sql)

      for(city<-list){
        pstm.setString(1,city.day)
        pstm.setString(2,city.city)
        pstm.setLong(3,city.times)
        pstm.addBatch()          //加入批处理操作
      }
      pstm.executeBatch()
      connection.commit()

    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
        MysqlUtil.release(connection,pstm)
    }
  }

  /**
    * 向数据库中插入统计的结果
    * @param list
    */
  def insertTimeTopn(list:ListBuffer[TimeAccessTopn]): Unit ={
    try{
      connection=MysqlUtil.getConnection()
      connection.setAutoCommit(false)       //设置为手动提交
      val  sql="insert into time_topn(hour,cnt) values(?,?)"
      pstm=connection.prepareStatement(sql)

      for(time<-list){
        pstm.setString(1,time.hour)
        pstm.setLong(2,time.cnt)

        pstm.addBatch()          //加入批处理操作
      }
      pstm.executeBatch()
      connection.commit()

    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      MysqlUtil.release(connection,pstm)
    }
  }

  def insertTrafficsTopn(list:ListBuffer[TrafficAccessTopn]): Unit ={
    try{
      connection=MysqlUtil.getConnection()
//      connection.setAutoCommit(false)       //设置为手动提交
      val  sql="insert into traffics_topn(day,url,traffics) values(?,?,?)"
      pstm=connection.prepareStatement(sql)

      for(traffic<-list){
        pstm.setString(1,traffic.day)
        pstm.setString(2,traffic.url)
        pstm.setDouble(3,traffic.traffics)
        pstm.execute()
//        pstm.addBatch()          //加入批处理操作
      }
//      pstm.executeBatch()
//      connection.commit()
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
//      MysqlUtil.release(connection,pstm)
    }
  }

}
