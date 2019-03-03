package org.spider.topupservices.DBOperations;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Initializations.SecretKey;
import org.spider.topupservices.Logs.LogWriter;

/**
 * @author hafiz
 *
 */
public class Login {
	WeTopUpDS weTopUpDS;
	LogWriter logWriter;
	/**
	 * 
	 */
	public Login(WeTopUpDS weTopUpDS, LogWriter logWriter) {
		this.weTopUpDS= weTopUpDS;
		this.logWriter=logWriter;
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
	 * @param loginCredential
	 * @param password
	 * @return
	 * 0:User verified
	 * 1:User credentials did not match
	 * E:General Exception
	 * -2:General Error at compareCredentialsInDB()
	 * -1: user not activated.
	 */
	public JsonEncoder compareCredentialsInDB(String loginCredential, String password) {
		String retval="-2";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String sql="select case when count(*)=0 then 1 else 0 end as counter, min(status) as status from users_info where (user_email=? or phone=?) and passwd_enc=AES_ENCRYPT(?,concat_ws('',?,key_seed,key_seed,key_seed))";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, loginCredential);
			loginCredential=this.msisdnNormalize(loginCredential);
			weTopUpDS.getPreparedStatement().setString(2, loginCredential);
			weTopUpDS.getPreparedStatement().setString(3, password);
			weTopUpDS.getPreparedStatement().setString(4, SecretKey.SECRETKEY);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				retval=rs.getString(1);
				LogWriter.LOGGER.info("User count:"+retval);
				//this.logWriter.appendLog("uc:"+retval);
				if(!(rs.getString(2).equals("1"))) {
					retval="-1";
					errorCode="-1";//default errorCode
					errorMessage = "User is not activated.";
				}
				errorCode="0";
				errorMessage = "User is activated.";
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
			if(weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closeResultSet();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
				try {
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
			}
			LogWriter.LOGGER.severe(e.getMessage());
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	
	
	
	/**
	 * 
	 * @param loginCredential
	 * @param password
	 * @param mode
	 * @return
	 * 0:User verified
	 * 1:User credentials did not match
	 * E:General Exception
	 * -2:General Error at compareCredentialsInDB()
	 * -1: user not activated.
	 */
	public JsonEncoder compareCredentialsInDB(String loginCredential, String password, int mode) {
		String retval="-2";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String sql="select case when count(*)=0 then 1 else 0 end as counter, min(status) as status from users_info where <mode>=? and passwd_enc=AES_ENCRYPT(?,concat_ws('',?,key_seed,key_seed,key_seed))";
		if(mode==1) { //email
			sql=sql.replace("<mode>", "user_email");
		}else { //phone
			sql=sql.replace("<mode>", "phone");
			loginCredential=this.msisdnNormalize(loginCredential);
		}
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, loginCredential);
			weTopUpDS.getPreparedStatement().setString(2, password);
			weTopUpDS.getPreparedStatement().setString(3, SecretKey.SECRETKEY);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				retval=rs.getString(1);
				LogWriter.LOGGER.info("User count:"+retval);
				//this.logWriter.appendLog("uc:"+retval);
				if(!(rs.getString(2).equals("1"))) {
					retval="-1";
					errorCode="-1";//default errorCode
					errorMessage = "User is not activated.";
				}
				errorCode="0";
				errorMessage = "User is activated.";
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
			if(weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closeResultSet();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
				try {
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
			}
			LogWriter.LOGGER.severe(e.getMessage());
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder checkUserInDB(String loginCredential) {
		String userFlag="-2";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String sql="select count(*) as counter from users_info where user_email=? or phone=?";
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, loginCredential);
			loginCredential=this.msisdnNormalize(loginCredential);
			weTopUpDS.getPreparedStatement().setString(2, loginCredential);
			
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				userFlag=rs.getString(1);
				errorCode="0";
				errorMessage = "checked user successfully.";
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
			if(weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closeResultSet();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
				try {
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
			}
			LogWriter.LOGGER.severe(e.getMessage());
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userFlag", userFlag);
		jsonEncoder.addElement("username", loginCredential);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder checkUserInDB(String email, String phone) {
		String userFlag="-2";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String sql="select count(*) as counter from users_info where user_email=? or phone=?";
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, email);
			phone=this.msisdnNormalize(phone);
			weTopUpDS.getPreparedStatement().setString(2, phone);
			
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				userFlag=rs.getString(1);
				errorCode="0";
				errorMessage = "checked user successfully.";
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
			if(weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closeResultSet();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
				try {
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
			}
			LogWriter.LOGGER.severe(e.getMessage());
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userFlag", userFlag);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
//	public JsonEncoder checkUserInDB(String loginCredential, int mode) {
//		String userFlag="-2";
//		JsonEncoder jsonEncoder = new JsonEncoder();
//		String errorCode="-2";//default errorCode
//		String errorMessage = "user not found.";
//		String sql="select count(*) as counter from users_info where <mode>=?";
//		if(mode==1) { //email
//			sql=sql.replace("<mode>", "user_email");
//		}else { //phone
//			sql=sql.replace("<mode>", "phone");
//			loginCredential=this.msisdnNormalize(loginCredential);
//		}
//		try {
//			weTopUpDS.prepareStatement(sql);
//			weTopUpDS.getPreparedStatement().setString(1, loginCredential);
//			ResultSet rs = weTopUpDS.executeQuery();
//			while (rs.next()) {
//				userFlag=rs.getString(1);
//				errorCode="0";
//				errorMessage = "checked user successfully.";
//			}
//			weTopUpDS.closeResultSet();
//			weTopUpDS.closePreparedStatement();
//		}catch(NullPointerException e) {
//			errorCode="-5";//default errorCode
//			errorMessage = "User does not exist.";
//			LogWriter.LOGGER.severe(e.getMessage());
//		}
//		catch(Exception e){
//			errorCode="-3";//default errorCode
//			errorMessage = "General Exception.";
//			if(weTopUpDS.getConnection() != null) {
//				try {
//					weTopUpDS.closeResultSet();
//				} catch (SQLException e1) {
//					LogWriter.LOGGER.severe(e1.getMessage());
//				}
//				try {
//					weTopUpDS.closePreparedStatement();
//				} catch (SQLException e1) {
//					LogWriter.LOGGER.severe(e1.getMessage());
//				}
//			}
//			LogWriter.LOGGER.severe(e.getMessage());
//		}
//		jsonEncoder.addElement("ErrorCode", errorCode);
//		jsonEncoder.addElement("ErrorMessage", errorMessage);
//		jsonEncoder.addElement("userFlag", userFlag);
//		jsonEncoder.addElement("username", loginCredential);
//		jsonEncoder.buildJsonObject();
//		
//		return jsonEncoder;
//	}
	
	public JsonEncoder updateUserKey(String email, String key) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "UPDATE users_info t SET t.key=? WHERE (t.user_email=?) and t.user_id > 0";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, key);
			weTopUpDS.getPreparedStatement().setString(2, email);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			errorCode = "0";
			errorMessage = "Update successful.";
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			errorCode = "11";
			errorMessage = "SQL Exception";
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder updateUserPassword(String key, String password) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String keySeed = fetchkeySeedByKey(key);
		String sql = "UPDATE users_info t SET t.passwd_enc=AES_ENCRYPT(?,concat_ws('',?,?,?,?)) WHERE t.key=? and t.user_id > 0";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, password);
			weTopUpDS.getPreparedStatement().setString(2, SecretKey.SECRETKEY);//key
			weTopUpDS.getPreparedStatement().setString(3, keySeed);
			weTopUpDS.getPreparedStatement().setString(4, keySeed);
			weTopUpDS.getPreparedStatement().setString(5, keySeed);
			weTopUpDS.getPreparedStatement().setString(6, key);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			errorCode = "0";
			errorMessage = "Update successful.";
			
			String userInfosql="SELECT u.user_id, u.user_name, case when u.user_email is null then '' else u.user_email end as user_email, u.user_type, u.phone, u.status FROM users_info u where u.key=?";

			try {
				weTopUpDS.prepareStatement(userInfosql);
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
					errorMessage = "update and fetch successful.";
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
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			errorCode = "11";
			errorMessage = "SQL Exception";
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder changeUserPassword(String credential, String newPassword) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		credential=this.msisdnNormalize(credential);
		
		String keySeed = fetchkeySeedByUser(credential);
		LogWriter.LOGGER.info("keySeed : "+keySeed);
		
		String sql = "UPDATE users_info t SET t.passwd_enc=AES_ENCRYPT(?,concat_ws('',?,?,?,?)) WHERE t.user_email=? or t.phone=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, newPassword);
			weTopUpDS.getPreparedStatement().setString(2, SecretKey.SECRETKEY);//key
			weTopUpDS.getPreparedStatement().setString(3, keySeed);
			weTopUpDS.getPreparedStatement().setString(4, keySeed);
			weTopUpDS.getPreparedStatement().setString(5, keySeed);
			weTopUpDS.getPreparedStatement().setString(6, credential);
			weTopUpDS.getPreparedStatement().setString(7, credential);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			errorCode = "0";
			errorMessage = "Update successful.";
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			errorCode = "11";
			errorMessage = "SQL Exception";
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public String fetchkeySeedByKey(String key) {
		String keySeed = "";
		String sql="select t.key_seed from users_info t where `key`=?";
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, key);
			
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				keySeed = rs.getString(1);
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			if(weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closeResultSet();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
				try {
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
			}
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return keySeed;
	}
	
	public String fetchkeySeedByUser(String credential) {
		String keySeed = "";
		String sql="select t.key_seed from users_info t where t.user_email=? or t.phone=?";
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, credential);
			weTopUpDS.getPreparedStatement().setString(2, credential);
			
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				keySeed = rs.getString(1);
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			if(weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closeResultSet();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
				try {
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
			}
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return keySeed;
	}
}
