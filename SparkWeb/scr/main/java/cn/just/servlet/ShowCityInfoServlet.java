package cn.just.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import cn.just.dao.CityTopnDao;
import cn.just.domain.TopnInfo;
import net.sf.json.JSONArray;

public class ShowCityInfoServlet extends HttpServlet{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private CityTopnDao cityTopnDao=null;
	private List<TopnInfo> cityList=null;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		cityTopnDao=new CityTopnDao();
		
		String day=req.getParameter("day");
		
		System.out.println("============day="+day);
		
		cityList=cityTopnDao.getListCity(day);
		
		JSONArray cityJson=JSONArray.fromObject(cityList);
		
		resp.setContentType("text/html;charset=utf-8");
		
		PrintWriter write=resp.getWriter();
		write.println(cityJson);
		write.flush();
		write.close();
		
		
		
		
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// TODO Auto-generated method stub
		super.doPost(req, resp);
	}
}
