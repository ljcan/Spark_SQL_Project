package cn.just.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlUtil {
    private static final String DRIVER_NAME="jdbc:mysql://localhost:3306/sparkSql?user=root&password=123456";
    private static Connection connection;
    private static PreparedStatement pstm;
    private static ResultSet resultSet;

    public static Connection getConnection(){
        try {
        	Class.forName("com.mysql.jdbc.Driver");
            connection=DriverManager.getConnection(DRIVER_NAME);
        }catch (Exception e){
            e.printStackTrace();
        }
        return connection;
    }


    public static void release(){
        try {
        	if(resultSet!=null) {
        		resultSet.close();
        	}
            if (pstm != null) {
                pstm.close();
            }
            if(connection!=null){
                connection.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(connection!=null){
                connection=null;    //help GC
            }
        }
    }

}
