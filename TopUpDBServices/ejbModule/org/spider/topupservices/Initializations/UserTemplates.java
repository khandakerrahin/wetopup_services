package org.spider.topupservices.Initializations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Logs.LogWriter;

public class UserTemplates {

	public HashMap<String, List<String>> map;

	public UserTemplates() {
		map=new HashMap<>();
		
	}
	public void getUserTemplates(WeTopUpDS dsConnection) {
		try {
			String sql="SELECT c.action, c.template_id, t.username, t.subject, t.template_name, t.template, t.variables, t.json_request, t.smsTemplate FROM template_table t left join template_configuration c on c.template_id=t.id;";
			dsConnection.prepareStatement(sql);
			ResultSet rs = dsConnection.executeQuery();
			map.clear();
			while (rs.next()) {
				List<String> templateInfo = new ArrayList<>();
				templateInfo.add(rs.getString("template_id"));
				templateInfo.add(rs.getString("username"));
				templateInfo.add(rs.getString("subject"));
				templateInfo.add(rs.getString("template_name"));
				templateInfo.add(rs.getString("template"));
				templateInfo.add(rs.getString("variables"));
				templateInfo.add(rs.getString("json_request"));
				templateInfo.add(rs.getString("smsTemplate"));
				map.put(rs.getString("action"), templateInfo);
			}
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
