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
					//this.logWriter.appendLog("ua:"+rs.getString(2));
					//this.logWriter.appendAdditionalInfo(errorMessage);
					
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
			//this.logWriter.setStatus(0);
			//this.logWriter.appendLog("uc:E");
			//this.logWriter.appendAdditionalInfo("Login.compareCred():"+e.getMessage());
		}
//		finally{
//			if(weTopUpDS.getConnection() != null){
//				try {
//					weTopUpDS.getConnection().close();
//				} catch (SQLException e) {
//					LogWriter.LOGGER.severe(e.getMessage());
//				}
//			}      
//		}
//		if(retval.equals("1")) //credentials valid
//		else if(retval.equals("0")); //username and password does not match
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder checkUserInDB(String loginCredential, int mode) {
		String userFlag="-2";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String sql="select count(*) as counter from users_info where <mode>=?";
		if(mode==1) { //email
			sql=sql.replace("<mode>", "user_email");
		}else { //phone
			sql=sql.replace("<mode>", "phone");
			loginCredential=this.msisdnNormalize(loginCredential);
		}
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, loginCredential);
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
}
