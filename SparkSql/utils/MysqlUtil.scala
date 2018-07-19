package cn.just.shinelon.sparkSql_Project.cn.just.shinelon.sparkSql_Project




import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * 连接Mysql工具类
  */
object MysqlUtil {
  def getConnection():Connection ={
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkSql?user=root&password=123456")
  }

  def release(connection:Connection,psmt:PreparedStatement): Unit ={
    try{
      if(psmt!=null){
        psmt.close()
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      if(connection!=null){
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
