package org.spider.topupservices.Initializations;

import java.sql.SQLException;
import java.util.HashMap;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Logs.LogWriter;



public class Configurations {
	WeTopUpDS dsConnection;
	TopUpUsers topUpUsers;
	public Configurations() {
		topUpUsers = new TopUpUsers();
	}

	public void loadConfigurationFromDB() {
		try {
		dsConnection=new WeTopUpDS();
		topUpUsers.getTopUpUsers(dsConnection);
		}finally{
			if(dsConnection.getConnection() != null){
//				this.logWriter.flush(dsConnection);
				try {
					dsConnection.getConnection().close();
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
				}
			}
		}
	}

	/**
	 * @return HashMap(rawtypes) replySMSLoader.replyMessage
	 */
	public HashMap<String,String> getTopUpUsers(){
		return  this.topUpUsers.map;
	}
	
}

