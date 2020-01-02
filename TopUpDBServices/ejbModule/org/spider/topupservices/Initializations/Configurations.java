package org.spider.topupservices.Initializations;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.spider.topupservices.Api.AuthenticationToken;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Logs.LogWriter;

import org.spider.topupservices.Initializations.UserTemplates;



public class Configurations {
	WeTopUpDS dsConnection;
	TopUpUsers topUpUsers;
	UserTemplates userTemplates;
	public static HashMap<String, AuthenticationToken> authenticationTokenHM = new HashMap<String,AuthenticationToken>();
    public static HashMap<String, String> checkValidUserToken = new HashMap<String,String>();
	
	public HashMap<String, AuthenticationToken> getAuthenticationTokenHM() {
		return authenticationTokenHM;
	}
	
	public static String generateNewTokenId(String userID, int validity) {
		String retval=null;
		AuthenticationToken at= new AuthenticationToken();
		String tokenId=at.generateNewTokenId(userID, validity);
		authenticationTokenHM.put(tokenId, at);
		retval=tokenId;
		return retval;
	}
	
	public Configurations() {
		topUpUsers = new TopUpUsers();
		userTemplates = new UserTemplates();
	}

	public void loadConfigurationFromDB() {
		try {
		dsConnection=new WeTopUpDS();
		topUpUsers.getTopUpUsers(dsConnection);
		userTemplates.getUserTemplates(dsConnection);
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
	public HashMap<String,List<String>> getTopUpUsers(){
		return  this.topUpUsers.map;
	}
	
	public HashMap<String,List<String>> getUserTemplates(){
		return  this.userTemplates.map;
	}
	
}

