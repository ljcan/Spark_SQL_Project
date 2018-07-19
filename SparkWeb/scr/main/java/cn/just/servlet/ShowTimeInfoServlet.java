package cn.just.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import cn.just.dao.CityTopnDao;
import cn.just.dao.TimeTopnDao;
import cn.just.domain.TopnInfo;
import net.sf.json.JSONArray;
/**
 * 用户点击量时间段分布情况
 * @author shinelon
 *
 */
public class ShowTimeInfoServlet extends HttpServlet{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private TimeTopnDao timeTopnDao=null;
	private List<TopnInfo> timeList=null;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		timeTopnDao=new TimeTopnDao();
		
		timeList=timeTopnDao.getListTime();
		
		JSONArray timeJson=JSONArray.fromObject(timeList);
		
		resp.setContentType("text/html;charset=utf-8");
		
		PrintWriter write=resp.getWriter();
		write.println(timeJson);
		write.flush();
		write.close();
		
		
		
		
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// TODO Auto-generated method stub
		super.doPost(req, resp);
	}
}
