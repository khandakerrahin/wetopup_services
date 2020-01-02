package org.spider.topupservices.DBOperations;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;

/**
 * @author hafiz
 *
 */
public class UserInfo {
	WeTopUpDS weTopUpDS;
	LogWriter logWriter;
	Configurations configurations;
	/**
	 * 
	 */
	public UserInfo(WeTopUpDS weTopUpDS,LogWriter logWriter, Configurations configurations) {
//		weTopUpDS= new WeTopUpDS(); 
		this.weTopUpDS=weTopUpDS;
		this.logWriter=logWriter;
		this.configurations=configurations;
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
	private String msisdnNormalizeOld(String msisdn) {
		if (msisdn.startsWith("0")) {
			msisdn = "88" + msisdn;
		}
		return msisdn;
	}
	
	private String msisdnNormalize(String phoneNumber) {
		try {
			phoneNumber = phoneNumber.replace("+", "").replaceAll("-", "").replaceAll(" ", "");
			//				phoneNumber = phoneNumber.replace("-", "");
			//				phoneNumber = phoneNumber.replace(" ", "");

			if(phoneNumber.matches("^((880)|(0))?(1[3-9]{1}|35|44|66){1}[0-9]{8}$")){
				//correct number of digits
				
				if(phoneNumber.startsWith("0")) phoneNumber = "88" + phoneNumber;
				else if(phoneNumber.startsWith("880")){

				}else {
					phoneNumber = "880" + phoneNumber;
				}
			}else {
				// invalid
			}
		} catch (Exception e) {
			e.printStackTrace();		
			}
		return phoneNumber;
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
		String sql="SELECT user_id, user_name, case when user_email is null then '' else user_email end as user_email, user_type, phone, status,balance,distributor_id,isPhoneVerified, dp_img, stock_configuration FROM users_info where (user_email=? or phone=?)";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			id=msisdnNormalize(id);
			weTopUpDS.getPreparedStatement().setString(2, id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				if(rs.getString("status").equals("10")) {
					errorCode="50";
					errorMessage = "User is blocked.";
				} else {
					jsonEncoder.addElement("user_id", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_id"))?"":rs.getString("user_id"));
					jsonEncoder.addElement("username", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_name"))?"":rs.getString("user_name"));
					jsonEncoder.addElement("email", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_email"))?"":rs.getString("user_email"));
					jsonEncoder.addElement("phoneNumber", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("phone"))?"":rs.getString("phone"));
					jsonEncoder.addElement("userType", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_type"))?"":rs.getString("user_type"));
					jsonEncoder.addElement("status", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("status"))?"":rs.getString("status"));
					jsonEncoder.addElement("balance", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("balance"))?"0":rs.getString("balance"));
					jsonEncoder.addElement("distributor_id", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("distributor_id"))?"":rs.getString("distributor_id"));
					jsonEncoder.addElement("isPhoneVerified", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("isPhoneVerified"))?"":rs.getString("isPhoneVerified"));
					jsonEncoder.addElement("dpUrl", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("dp_img"))?"":rs.getString("dp_img"));
					jsonEncoder.addElement("stock_configuration", rs.getString("stock_configuration"));
					
					errorCode="0";
					errorMessage = "fetch successful.";
				}
				
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
	
	public JsonEncoder fetchAdminInfo(String id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage="default error.";//default errorCode
		String sql="SELECT user_id, user_name, case when user_email is null then '' else user_email end as user_email, phone, status,dp_img FROM admins_info where (user_email=? or phone=?)";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			id=msisdnNormalize(id);
			weTopUpDS.getPreparedStatement().setString(2, id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				if(rs.getString("status").equals("10")) {
					errorCode="50";
					errorMessage = "Admin is blocked.";
				} else {
					jsonEncoder.addElement("user_id", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_id"))?"":rs.getString("user_id"));
					jsonEncoder.addElement("username", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_name"))?"":rs.getString("user_name"));
					jsonEncoder.addElement("email", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_email"))?"":rs.getString("user_email"));
					jsonEncoder.addElement("phoneNumber", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("phone"))?"":rs.getString("phone"));
					jsonEncoder.addElement("status", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("status"))?"":rs.getString("status"));
					jsonEncoder.addElement("dpUrl", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("dp_img"))?"":rs.getString("dp_img"));
					
					errorCode="0";
					errorMessage = "fetch successful.";
				}
				
				this.logWriter.setStatus(1);
				this.logWriter.appendLog("fu:S");
			}else {
				errorCode="-9"; //User details could not be retrieved
				errorMessage = "Admin details could not be retrieved.";
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
			this.logWriter.appendAdditionalInfo("UserInfo.fetchAdminInfo():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:E");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.fetchAdminInfo():"+e.getMessage());
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchUserInfoApi(String id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String user_id = "";
		int validity = 0;
		String sql="SELECT user_id, balance, api_token_validity FROM users_info where (user_email=? or phone=?)";
		LogWriter.LOGGER.info("id : "+id);
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			id=msisdnNormalize(id);
			weTopUpDS.getPreparedStatement().setString(2, id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				user_id = rs.getString("user_id");
				validity = rs.getInt("api_token_validity");
				LogWriter.LOGGER.info("validity : "+validity +" seconds");
				jsonEncoder.addElement("user_id", user_id);
				jsonEncoder.addElement("balance", rs.getString("balance"));
				
				errorCode="0000";
			}else {
				errorCode="0020";
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		}catch(SQLException e){
			errorCode= "0020";
			LogWriter.LOGGER.severe(e.getMessage());
		}catch(Exception e){
			errorCode= "0020";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		if(Configurations.checkValidUserToken.containsKey(user_id)) {
			String oldToken=Configurations.checkValidUserToken.get(user_id);
			Configurations.checkValidUserToken.remove(user_id);
			
			if(Configurations.authenticationTokenHM.containsKey(oldToken)) 
			Configurations.authenticationTokenHM.remove(oldToken);
			
			//LogWriter.LOGGER.info("inside validity check after " + oldToken);
		}
		
		if(errorCode.equals("0000")) {
			// add token
			//AuthenticationToken at= new AuthenticationToken();
			//String tokenId=at.generateNewTokenId(user_id);
			//TODO check if it always returns token or not 
			String tokenn= Configurations.generateNewTokenId(user_id, validity);
			Configurations.checkValidUserToken.put(user_id, tokenn);
			jsonEncoder.addElement("token", tokenn);
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
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
	
	/**
	 * 
	 * @param user_id
	 * @return
	 */
	public JsonEncoder fetchUserBalanceApi(String user_id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="0020";//User info fetch failure
		
		String sql="SELECT balance FROM users_info where (user_email=? or phone=?)";
		LogWriter.LOGGER.info("id : "+user_id);
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, user_id);
			String id=msisdnNormalize(user_id);
			weTopUpDS.getPreparedStatement().setString(2, id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
			
				jsonEncoder.addElement("balance", rs.getString("balance"));
				
				errorCode="0000";
			}else {
				errorCode="0020";
			}
		}catch(Exception e) {
			e.printStackTrace();
			errorCode="0031"; // Exception occurred while getting SQL result
		}
		jsonEncoder.addElement("ErrorCode", errorCode);		
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
		
	/**
	 * 
	 * @param user_id
	 * @return
	 */
	public JsonEncoder fetchUserBalanceApiToken(String user_id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="0020";//User info fetch failure
		
		String sql="SELECT balance FROM users_info where user_id=?";
		LogWriter.LOGGER.info("id : "+user_id);
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, user_id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
			
				jsonEncoder.addElement("balance", rs.getString("balance"));
				
				errorCode="0000";
			}else {
				errorCode="0020";
			}
		}catch(Exception e) {
			e.printStackTrace();
			errorCode="0025"; // Exception occurred while getting SQL result
		}
		jsonEncoder.addElement("ErrorCode", errorCode);		
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder fetchUserBalance(String user_id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage="default error.";//default errorCode
		
		if (NullPointerExceptionHandler.isNullOrEmpty(user_id)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		}else {
			String stockConfigurations = new UserDBOperations(weTopUpDS, configurations, logWriter).fetchUserStockConfigurations(user_id);
			// check if Stock visibility allowed for user
			// 0 = inactive; 1 = active;		bitwise	:		0 = visibility;	1 = history;	2 = topup;	3 = refill;	4 = transfer;
			if (stockConfigurations.charAt(0) == '0') {
				LogWriter.LOGGER.info("Stock visibility not permitted.");
				jsonEncoder.addElement("user_id", user_id);
				jsonEncoder.addElement("balance", "0");
				errorCode="0";
				errorMessage = "fetch successful.";
			} else {
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
//				finally{
//					if(weTopUpDS.getConnection() != null){
//						try {
//							weTopUpDS.getConnection().close();
//						} catch (SQLException e) {
//							errorCode="-4";
//							LogWriter.LOGGER.severe(e.getMessage());
//						}
//					}      
//				}
			}
			
			
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchUserByKey(String key) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage="default error.";//default errorCode
		String sql="SELECT u.user_id, case when u.user_name is null then '' else u.user_name end as user_name, case when u.user_email is null then '' else u.user_email end as user_email, u.user_type, u.phone, u.status,u.isPhoneVerified FROM users_info u where u.key=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, key);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				jsonEncoder.addElement("id", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_id"))?"":rs.getString("user_id"));
				jsonEncoder.addElement("username", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_name"))?"":rs.getString("user_name"));
				jsonEncoder.addElement("email", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_email"))?"":rs.getString("user_email"));
				jsonEncoder.addElement("phoneNumber", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("phone"))?"":rs.getString("phone"));
				jsonEncoder.addElement("userType", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_type"))?"":rs.getString("user_type"));
				jsonEncoder.addElement("status", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("status"))?"0":rs.getString("status"));
				jsonEncoder.addElement("isPhoneVerified", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("isPhoneVerified"))?"0":rs.getString("isPhoneVerified"));
				
				
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
	
	public JsonEncoder fetchUserStatus(String user_id) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage="default error.";//default errorCode
		String sql="SELECT status FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, user_id);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				jsonEncoder.addElement("user_id", user_id);
				jsonEncoder.addElement("status", rs.getString("status"));
				
				errorCode="0";
				errorMessage = "fetch successful.";
				this.logWriter.setStatus(1);
				this.logWriter.appendLog("fu:S");
			}else {
				errorCode="-9"; //User details could not be retrieved
				errorMessage = "User status could not be retrieved.";
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
			this.logWriter.appendAdditionalInfo("UserInfo.status():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("fu:E");
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.appendAdditionalInfo("UserInfo.status():"+e.getMessage());
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
