package org.spider.topupservices.Initializations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Logs.LogWriter;

public class TopUpUsers {

	public HashMap<String, List<String>> map;
	
	public TopUpUsers() {
		map=new HashMap<>();
		
	}
	public void getTopUpUsers(WeTopUpDS dsConnection) {
		try {
			//LogWriter.LOGGER.info("executing getTopUpUser()");
			String sql="SELECT appname,password,type,token_validity FROM app_info where status=10";
			dsConnection.prepareStatement(sql);
			ResultSet rs = dsConnection.executeQuery();
			map.clear();
			while (rs.next()) {
				//LogWriter.LOGGER.info("appName : "+rs.getString("appname")+" password : "+rs.getString("password"));
				
				List<String> appInfo = new ArrayList<>();
				appInfo.add(rs.getString("password"));
				appInfo.add(rs.getString("type"));
				appInfo.add(rs.getString("token_validity"));
				
				map.put(rs.getString("appname"), appInfo);
				
			}
			//LogWriter.LOGGER.info("executed getTopUpUser()");
		}catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		finally{
			if(dsConnection.getConnection() != null){
				try {
					dsConnection.closeResultSet();
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
				}
				try {
					dsConnection.closePreparedStatement();
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
				}
			}      
		}
	}
}
