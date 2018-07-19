package cn.just.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import cn.just.domain.TopnInfo;
import cn.just.util.MysqlUtil;

/**
 * 网站点击量时间段分布情况
 * @author shinelon
 *
 */
public class TimeTopnDao {
	private static Connection connection;
	private static PreparedStatement pstm;
	private static ResultSet resultSet;
	
	/**
	 * 查询网站点击量时间段分布情况
	 * @param day
	 * @return
	 */
	public static List<TopnInfo> getListTime(){
		List<TopnInfo> list=new ArrayList<TopnInfo>();
		try {
		connection=MysqlUtil.getConnection();
		String sql="select hour,cnt from time_topn order by cnt desc";
		pstm=connection.prepareStatement(sql);
		resultSet=pstm.executeQuery();
		TopnInfo timeTopn=null;
		while(resultSet.next()) {
			timeTopn=new TopnInfo();
			timeTopn.setName(resultSet.getString("hour"));
			timeTopn.setValue(resultSet.getLong("cnt"));
			list.add(timeTopn);
		}
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			MysqlUtil.release();
		}
		return list;
	}

}
