package cn.just.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import cn.just.domain.TopnInfo;
import cn.just.util.MysqlUtil;

public class CityTopnDao {
	private static Connection connection;
	private static PreparedStatement pstm;
	private static ResultSet resultSet;
	
	/**
	 * 按天查询一天中网站访问量中的城市信息
	 * @param day
	 * @return
	 */
	public static List<TopnInfo> getListCity(String day){
		List<TopnInfo> list=new ArrayList<TopnInfo>();
		try {
		connection=MysqlUtil.getConnection();
		String sql="select city,time from city_topn where day=? order by  time desc";
		pstm=connection.prepareStatement(sql);
		pstm.setString(1, day);
		resultSet=pstm.executeQuery();
		TopnInfo cityTopn=null;
		while(resultSet.next()) {
			cityTopn=new TopnInfo();
			cityTopn.setName(resultSet.getString("city"));
			cityTopn.setValue(resultSet.getLong("time"));
			list.add(cityTopn);
		}
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			MysqlUtil.release();
		}
		return list;
	}
	
//	public static void main(String[] args) {
//		List<CityTopn> list=new ArrayList<CityTopn>();
//		list=getListCity("20150831");
//		for(CityTopn city:list) {
//			System.out.println(city.toString());
//		}
//	}
	
}
