package org.spider.topupservices.DBOperations;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Logs.LogWriter;

/**
 * @author hafiz
 *
 */
public class UserInfo {
	WeTopUpDS weTopUpDS;
	LogWriter logWriter;
	/**
	 * 
	 */
	public UserInfo(WeTopUpDS weTopUpDS,LogWriter logWriter) {
//		weTopUpDS= new WeTopUpDS(); 
		this.weTopUpDS=weTopUpDS;
		this.logWriter=logWriter;
	}
	
	public JsonEncoder listUsers() {
		JsonEncoder jsonUserList=new JsonEncoder();
		
		return jsonUserList;		
	}
	/**
	 * If msisdn starts with 0, prepends 88.
	 * If msisdn starts with 880 or any other number, returns the String
	 * @param msisdn
	 * @return msisdn of the format 8801xx
	 */
	private String msisdnNormalize(String msisdn) {
		if(msisdn.startsWith("0")) {
			msisdn="88"+msisdn;
		}
		return msisdn;
	}
	/**
	 * 
	 * @param id
	 * @return jsonEncoder userInfo on success
	 * <br>errorCode 0 indicated success in fetching data
	 * <br>-1:General Error
	 * <br>-2:SQLException
	 * <br>-3:Exception
	 * <br>-4:SQLException while closing
	 */
	public JsonEncoder fetchUserInfo(String id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage="default error.";//default errorCode
		String sql="SELECT user_id, user_name, case when user_email is null then '' else user_email end as user_email, user_type, phone, status,balance,distributor_id FROM users_info where (user_email=? or phone=?)";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			id=msisdnNormalize(id);
			weTopUpDS.getPreparedStatement().setString(2, id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				jsonEncoder.addElement("id", rs.getString("user_id"));
				jsonEncoder.addElement("username", rs.getString("user_name"));
				jsonEncoder.addElement("email", rs.getString("user_email"));
				jsonEncoder.addElement("phoneNumber", rs.getString("phone"));
				jsonEncoder.addElement("userType", rs.getString("user_type"));
				jsonEncoder.addElement("status", rs.getString("status"));
				jsonEncoder.addElement("balance", rs.getString("balance"));
				jsonEncoder.addElement("distributor_id", rs.getString("distributor_id"));
				
				errorCode="0";
				errorMessage = "fetch successful.";
				this.logWriter.setStatus(1);
				this.logWriter.appendLog("fu:S");
			}else {
				errorCode="-9"; //User details could not be retrieved
				errorMessage = "User details could not be retrieved.";
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("fu:F");
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(SQLException e){
			errorCode= "-2";
			errorMessage = "SQLException.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:SE");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserInfo():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:E");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserInfo():"+e.getMessage());
		}
//		finally{
//			if(weTopUpDS.getConnection() != null){
//				try {
//					weTopUpDS.getConnection().close();
//				} catch (SQLException e) {
//					errorCode="-4";
//					LogWriter.LOGGER.severe(e.getMessage());
//				}
//			}      
//		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchUserEmail(String id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage="default error.";//default errorCode
		String sql="SELECT case when u.user_email is null then '' else u.user_email end as user_email, u.phone FROM users_info u where (u.user_email=? or u.phone=?)";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			id=msisdnNormalize(id);
			weTopUpDS.getPreparedStatement().setString(2, id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				jsonEncoder.addElement("email", rs.getString("user_email"));
				jsonEncoder.addElement("phoneNumber", rs.getString("phone"));
				
				errorCode="0";
				errorMessage = "fetch successful.";
				this.logWriter.setStatus(1);
				this.logWriter.appendLog("fu:S");
			}else {
				errorCode="-9"; //User details could not be retrieved
				errorMessage = "User details could not be retrieved.";
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("fu:F");
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(SQLException e){
			errorCode= "-2";
			errorMessage = "SQLException.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:SE");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserInfo():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:E");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserInfo():"+e.getMessage());
		}
//		finally{
//			if(weTopUpDS.getConnection() != null){
//				try {
//					weTopUpDS.getConnection().close();
//				} catch (SQLException e) {
//					errorCode="-4";
//					LogWriter.LOGGER.severe(e.getMessage());
//				}
//			}      
//		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchUserBalance(String user_id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage="default error.";//default errorCode
		String sql="SELECT user_id, balance FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, user_id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				jsonEncoder.addElement("user_id", rs.getString("user_id"));
				jsonEncoder.addElement("balance", rs.getString("balance"));
				
				errorCode="0";
				errorMessage = "fetch successful.";
				this.logWriter.setStatus(1);
				this.logWriter.appendLog("fu:S");
			}else {
				errorCode="-9"; //User details could not be retrieved
				errorMessage = "balance could not be retrieved.";
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("fu:F");
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(SQLException e){
			errorCode= "-2";
			errorMessage = "SQLException.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:SE");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserBalance():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:E");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserBalance():"+e.getMessage());
		}
//		finally{
//			if(weTopUpDS.getConnection() != null){
//				try {
//					weTopUpDS.getConnection().close();
//				} catch (SQLException e) {
//					errorCode="-4";
//					LogWriter.LOGGER.severe(e.getMessage());
//				}
//			}      
//		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchUserByKey(String key) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage="default error.";//default errorCode
		String sql="SELECT u.user_id, u.user_name, case when u.user_email is null then '' else u.user_email end as user_email, u.user_type, u.phone, u.status FROM users_info u where u.key=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, key);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				jsonEncoder.addElement("id", rs.getString("user_id"));
				jsonEncoder.addElement("username", rs.getString("user_name"));
				jsonEncoder.addElement("email", rs.getString("user_email"));
				jsonEncoder.addElement("phoneNumber", rs.getString("phone"));
				jsonEncoder.addElement("userType", rs.getString("user_type"));
				jsonEncoder.addElement("status", rs.getString("status"));
				
				errorCode="0";
				errorMessage = "fetch successful.";
				this.logWriter.setStatus(1);
				this.logWriter.appendLog("fu:S");
			}else {
				errorCode="-9"; //User details could not be retrieved
				errorMessage = "User details could not be retrieved.";
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("fu:F");
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(SQLException e){
			errorCode= "-2";
			errorMessage = "SQLException.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:SE");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserInfo():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:E");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserInfo():"+e.getMessage());
		}
//		finally{
//			if(weTopUpDS.getConnection() != null){
//				try {
//					weTopUpDS.getConnection().close();
//				} catch (SQLException e) {
//					errorCode="-4";
//					LogWriter.LOGGER.severe(e.getMessage());
//				}
//			}      
//		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

}
