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
	 * @param mode 1:email, 2:phone
	 * @return jsonEncoder userInfo on success
	 * <br>errorCode 0 indicated success in fetching data
	 * <br>-1:General Error
	 * <br>-2:SQLException
	 * <br>-3:Exception
	 * <br>-4:SQLException while closing
	 */
	public JsonEncoder fetchUserInfo(String id, String mode) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String sql="SELECT u.user_id, u.user_name, case when u.user_email is null then '' else u.user_email end as user_email, u.user_type, u.phone, u.status FROM users_info u where u.<mode>=?";
		if(mode.equals("1")) { //email
			sql=sql.replace("<mode>", "user_email");
		}else { //phone
			sql=sql.replace("<mode>", "phone");
			id=msisdnNormalize(id);
		}
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				jsonEncoder.addElement("id", rs.getString("user_id"));
				jsonEncoder.addElement("username", rs.getString("user_name"));
				jsonEncoder.addElement("email", rs.getString("user_email"));
				jsonEncoder.addElement("phoneNumber", rs.getString("phone"));
				jsonEncoder.addElement("userType", rs.getString("user_type"));
				jsonEncoder.addElement("status", rs.getString("status"));
				
				errorCode="0";
				this.logWriter.setStatus(1);
				this.logWriter.appendLog("fu:S");
			}else {
				errorCode="-9:User details could not be retrieved";
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("fu:F");
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(SQLException e){
			errorCode= "-2";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:SE");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchUserInfo():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
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
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

}
