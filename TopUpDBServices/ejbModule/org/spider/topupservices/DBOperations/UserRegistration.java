package org.spider.topupservices.DBOperations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.Initializations.SecretKey;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.DBOperations.UserDBOperations;

import org.spider.topupservices.Utilities.NullPointerExceptionHandler;
import org.spider.topupservices.Utilities.RandomStringGenerator;

/**
 * @author hafiz
 *
 */
public class UserRegistration {
	WeTopUpDS weTopUpDS;
	LogWriter logWriter;
	Configurations configurations;
	/**
	 * 
	 */
	public UserRegistration(WeTopUpDS weTopUpDS,LogWriter logWriter,Configurations configurations) {
//		weTopUpDS = new WeTopUpDS();
		this.weTopUpDS=weTopUpDS;
		this.logWriter=logWriter;
		this.configurations=configurations;
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
	 * @param userId
	 * @return 0:Successfully Inserted
	 * 
	 */
	public String insertToCustomerBalanceTable(String userId) {
		String retval= "-1";
		double balance=0.0;
		String sql="INSERT INTO user_balance (user_id,balance) VALUES (?, ?)";
		try {
			weTopUpDS.prepareStatement(sql,true);
			weTopUpDS.getPreparedStatement().setString(1, userId);
			weTopUpDS.getPreparedStatement().setDouble(2, balance);

			retval="0:Successfully Inserted";
			weTopUpDS.execute();
		}catch(Exception e) {				
			e.printStackTrace();
		}finally {
			try {
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}			
		return retval;		
	}

	/**
	 * 
	 * @param userId
	 * @return 0:Successfully Inserted
	 * -1 failed
	 */
	public String insertToTblChargingTable(String userId,String rate) {
		String retval= "-1";		
		String sql="INSERT INTO commission_configurations (user_id,cash_rate) VALUES (?,?)";
		try {
			weTopUpDS.prepareStatement(sql,true);
			weTopUpDS.getPreparedStatement().setString(1, userId);
			weTopUpDS.getPreparedStatement().setString(2, rate);
			
			retval="0:Successfully Inserted";
			weTopUpDS.execute();
		}catch(Exception e) {				
			e.printStackTrace();
		}finally {
			try {
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}			
		return retval;		
	}
	
	/**
	 * 
	 * @param userId
	 * @return 0:Successfully Inserted
	 * -1 failed
	 */
	public String insertToTblChargingTable(String userId) {
		String retval= "-1";		
		String sql="INSERT INTO commission_configurations (user_id) VALUES (?)";
		try {
			weTopUpDS.prepareStatement(sql,true);
			weTopUpDS.getPreparedStatement().setString(1, userId);

			retval="0:Successfully Inserted";
			weTopUpDS.execute();
		}catch(Exception e) {				
			e.printStackTrace();
		}finally {
			try {
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}			
		return retval;		
	}
	
	/**
	 * 
	 * @param jsonDecoder user_name, phone, user_email, user_type, password
	 * @return 0:Successfully Inserted
	 * <br>1:User with the email address exists
	 * <br>2:Inserting organization details failed
	 * <br>11:Inserting user credentials failed
	 * <br>E:JSON string invalid
	 * <br>-1:Default Error Code
	 * <br>-2:SQLException
	 * <br>-3:General Exception
	 * <br>-4:SQLException while closing connection
	 */
	public JsonEncoder registerNewUser(JsonDecoder jsonDecoder) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage = "default error message.";
		String email = jsonDecoder.getJsonObject().getString("email");
		String phone = this.msisdnNormalize(jsonDecoder.getEString("phone"));
		//user_name, phone, user_email, user_type, password
		
		String sqlInsertusers_info="INSERT INTO users_info("
				+ "user_auth_token,user_name,user_email,user_type,status,phone,key_seed,api_key_seed,passwd_enc)" 
				+ "VALUES" 
				+ "(?,?,?,'1',1,?,?,?,AES_ENCRYPT(?,concat_ws('',?,?,?,?)))";

		String userId="-1";
		try {
			//json: username,email,phone,password
			weTopUpDS.prepareStatement(sqlInsertusers_info,true);
			String keySeed=jsonDecoder.getJsonObject().getString("email")+this.msisdnNormalize(jsonDecoder.getEString("phone"));
			String apiKeySeed = RandomStringGenerator.getRandomString("0123456789abcdefghijkl@.mnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",24);
			String userAuthToken = RandomStringGenerator.getRandomString("1234567890ABEDEFGHIJKLMNOPQRSTUVWXYZ.~$@*!-abcdefghijklmnopqrstuvwxyz",24);
			
			weTopUpDS.getPreparedStatement().setString(1, userAuthToken);//user_auth_token
			weTopUpDS.getPreparedStatement().setString(2, jsonDecoder.getJsonObject().getString("username"));
			weTopUpDS.getPreparedStatement().setString(3, email);
			weTopUpDS.getPreparedStatement().setString(4, phone);
			weTopUpDS.getPreparedStatement().setString(5, keySeed);//key_seed
			weTopUpDS.getPreparedStatement().setString(6, apiKeySeed);//api_key_seed
			weTopUpDS.getPreparedStatement().setString(7, jsonDecoder.getJsonObject().getString("password"));//AES encrypt password
			weTopUpDS.getPreparedStatement().setString(8, SecretKey.SECRETKEY);//key
			weTopUpDS.getPreparedStatement().setString(9, keySeed);
			weTopUpDS.getPreparedStatement().setString(10, keySeed);
			weTopUpDS.getPreparedStatement().setString(11, keySeed);
			boolean insertSuccess=false;
			try{ 
				weTopUpDS.execute();
				insertSuccess=true;
				
				userId=getUserId();
				if(!userId.equalsIgnoreCase("-1")) { //not -1 means user created successfully
					//insertToCustomerBalanceTable(userId);
					insertToTblChargingTable(userId);

					errorCode = "0";
					errorMessage = "Successfully registered.";
					
					try {
//						send email
						new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("registrationSuccess", phone, email, null);
					}catch(Exception e) {
						e.printStackTrace();
					}
					if(jsonDecoder.isParameterPresent("trx_id")) {
						if(NullPointerExceptionHandler.isNullOrEmpty(jsonDecoder.getNString("trx_id"))) {
							
						}else {
//							LogWriter.LOGGER.info("userId : "+userId);
//							LogWriter.LOGGER.info("TRX_D : "+jsonDecoder.getNString("trx_id"));
							updateCardListUser(userId,jsonDecoder.getNString("trx_id"));
						}
					}
					
					
				}else {
					LogWriter.LOGGER.info("user insert/create failed");

					errorCode = "-1";
					errorMessage = "failed to register user.";
				}
			}catch(SQLIntegrityConstraintViolationException de) {
				errorCode = "1";
				errorMessage = "User with the email address or phone number exists.";
				LogWriter.LOGGER.info("SQLIntegrityConstraintViolationException:"+de.getMessage());
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("rs:SIVE");
				this.logWriter.appendAdditionalInfo(errorCode);
			}catch(SQLException e) {
				errorCode = "11";
				errorMessage = "SQLException.";
				LogWriter.LOGGER.severe("SQLException"+e.getMessage());
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("rs:SE");
				this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
			}
			LogWriter.LOGGER.info("UserID:"+userId);
			
		}catch(SQLException e){
			errorCode= "11";
			errorMessage = "SQLException.";
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("rs:SE11");
			this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
//			e.printStackTrace();
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("rs:E-3");
			this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		if(errorCode.equals("0")) {
			return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserInfo(phone);
		}
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder registerNewAppUser(JsonDecoder jsonDecoder, String source) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage = "default error message.";
		String email = jsonDecoder.getNString("email");
		String phone = this.msisdnNormalize(jsonDecoder.getEString("phone"));
		String password = jsonDecoder.getNString("password");
		
		if (NullPointerExceptionHandler.isNullOrEmpty(phone) && NullPointerExceptionHandler.isNullOrEmpty(email)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			if(NullPointerExceptionHandler.isNullOrEmpty(password)) {
				password = RandomStringGenerator.getRandomString("0123456789abcdefghijkl@.mnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",8);
			}
			
			//user_name, phone, user_email, user_type, password
			
			String sqlInsertusers_info="INSERT INTO users_info("
					+ "user_auth_token,user_name,user_email,user_type,status,phone,key_seed,api_key_seed,passwd_enc)" 
					+ "VALUES" 
					+ "(?,?,?,'1',1,?,?,?,AES_ENCRYPT(?,concat_ws('',?,?,?,?)))";

			String userId="-1";
			try {
				//json: username,email,phone,password
				weTopUpDS.prepareStatement(sqlInsertusers_info,true);
				String keySeed = source +"_"+ RandomStringGenerator.getRandomString("0123456789abcdefghijkl@.mnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",24);
				String apiKeySeed = RandomStringGenerator.getRandomString("0123456789abcdefghijkl@.mnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",24);
				String userAuthToken = RandomStringGenerator.getRandomString("1234567890ABEDEFGHIJKLMNOPQRSTUVWXYZ.~$@*!-abcdefghijklmnopqrstuvwxyz",24);
				
				weTopUpDS.getPreparedStatement().setString(1, userAuthToken);//user_auth_token
				
				if(NullPointerExceptionHandler.isNullOrEmpty(jsonDecoder.getNString("username"))) {
					weTopUpDS.getPreparedStatement().setNull(2, Types.INTEGER);
				} else {
					weTopUpDS.getPreparedStatement().setString(2, jsonDecoder.getNString("username"));
				}
				
				if(NullPointerExceptionHandler.isNullOrEmpty(email)) {
					weTopUpDS.getPreparedStatement().setNull(3, Types.INTEGER);
				} else {
					weTopUpDS.getPreparedStatement().setString(3, email);
				}
				if(NullPointerExceptionHandler.isNullOrEmpty(phone)) {
					weTopUpDS.getPreparedStatement().setNull(4, Types.INTEGER);
				} else {
					weTopUpDS.getPreparedStatement().setString(4, phone);
				}
				
				weTopUpDS.getPreparedStatement().setString(5, keySeed);//key_seed
				weTopUpDS.getPreparedStatement().setString(6, apiKeySeed);//api_key_seed
				weTopUpDS.getPreparedStatement().setString(7, password);//AES encrypt password
				weTopUpDS.getPreparedStatement().setString(8, SecretKey.SECRETKEY);//key
				weTopUpDS.getPreparedStatement().setString(9, keySeed);
				weTopUpDS.getPreparedStatement().setString(10, keySeed);
				weTopUpDS.getPreparedStatement().setString(11, keySeed);
				boolean insertSuccess=false;
				try{ 
					weTopUpDS.execute();
					insertSuccess=true;
					
					userId=getUserId();
					if(!userId.equalsIgnoreCase("-1")) { //not -1 means user created successfully
						//insertToCustomerBalanceTable(userId);
						insertToTblChargingTable(userId);

						errorCode = "0";
						errorMessage = "Successfully registered.";
						
						if(NullPointerExceptionHandler.isNullOrEmpty(email)) {
							
						} else {
							try {
//								send email
								new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("registrationSuccess", phone, email, null);
							}catch(Exception e) {
								e.printStackTrace();
							}
						}
					}else {
						LogWriter.LOGGER.info("user insert/create failed");

						errorCode = "-1";
						errorMessage = "failed to register user.";
					}
				}catch(SQLIntegrityConstraintViolationException de) {
					errorCode = "1";
					errorMessage = "User with the email address or phone number exists.";
					LogWriter.LOGGER.info("SQLIntegrityConstraintViolationException:"+de.getMessage());
					this.logWriter.setStatus(0);
					this.logWriter.appendLog("rs:SIVE");
					this.logWriter.appendAdditionalInfo(errorCode);
				}catch(SQLException e) {
					errorCode = "11";
					errorMessage = "SQLException.";
					LogWriter.LOGGER.severe("SQLException"+e.getMessage());
					this.logWriter.setStatus(0);
					this.logWriter.appendLog("rs:SE");
					this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
				}
				LogWriter.LOGGER.info("UserID:"+userId);
				
			}catch(SQLException e){
				errorCode= "11";
				errorMessage = "SQLException.";
				LogWriter.LOGGER.severe(e.getMessage());
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("rs:SE11");
				this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
			}catch(Exception e){
				errorCode= "-3";
				errorMessage = "General Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
				e.printStackTrace();
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("rs:E-3");
				this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
			}
		}
			
		
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);

//		send OTP
//		if(errorCode.equals("0")) {
////			return new UserInfo(this.weTopUpDS,this.logWriter).fetchUserInfo(phone);
//			return new Login(this.weTopUpDS,this.configurations,this.logWriter).requestSmsOTP(phone);
//		}
		
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	/**
	 * 
	 * @param jsonDecoder user_name, phone, user_email, user_type, password
	 * @return 0:Successfully Inserted
	 * <br>1:User with the email address exists
	 * <br>2:Inserting organization details failed
	 * <br>11:Inserting user credentials failed
	 * <br>E:JSON string invalid
	 * <br>-1:Default Error Code
	 * <br>-2:SQLException
	 * <br>-3:General Exception
	 * <br>-4:SQLException while closing connection
	 */
	public JsonEncoder registerNewRetailer(JsonDecoder jsonDecoder) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage = "default error message.";
		
		//user_name, phone, user_email, user_type, password
		
		String sqlInsertusers_info="INSERT INTO users_info("
				+ "user_auth_token,user_name,user_email,user_type,status,phone,key_seed,api_key_seed,passwd_enc,distributor_id,address,dp_img,doc_img_01,doc_img_02,doc_img_03)" 
				+ "VALUES" 
				+ "(?,?,?,'5',1,?,?,?,AES_ENCRYPT(?,concat_ws('',?,?,?,?)),?,?,?,?,?,?)";

		String userId="-1";
		
		String keySeed = NullPointerExceptionHandler.isNullOrEmpty(jsonDecoder.getJsonObject().getString("email"))?RandomStringGenerator.getRandomString("0123456789abcdefghijkl@.mnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",10):jsonDecoder.getJsonObject().getString("email");
		String apiKeySeed = RandomStringGenerator.getRandomString("0123456789abcdefghijkl@.mnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",24);
		String userAuthToken = RandomStringGenerator.getRandomString("1234567890ABEDEFGHIJKLMNOPQRSTUVWXYZ.~$@*!-abcdefghijklmnopqrstuvwxyz",24);
		
		
		try {
			String rate = jsonDecoder.getJsonObject().getString("rate");
			//json: username,email,phone,password
			weTopUpDS.prepareStatement(sqlInsertusers_info,true);
			keySeed = keySeed + this.msisdnNormalize(jsonDecoder.getEString("phone"));
			weTopUpDS.getPreparedStatement().setString(1, userAuthToken);//user_auth_token
			weTopUpDS.getPreparedStatement().setString(2, jsonDecoder.getJsonObject().getString("username"));
			weTopUpDS.getPreparedStatement().setString(3, jsonDecoder.getJsonObject().getString("email"));
			weTopUpDS.getPreparedStatement().setString(4, this.msisdnNormalize(jsonDecoder.getEString("phone")));
			weTopUpDS.getPreparedStatement().setString(5, keySeed);//key_seed
			weTopUpDS.getPreparedStatement().setString(6, apiKeySeed);//key_seed
			weTopUpDS.getPreparedStatement().setString(7, jsonDecoder.getJsonObject().getString("password"));
			weTopUpDS.getPreparedStatement().setString(8, SecretKey.SECRETKEY);//key
			weTopUpDS.getPreparedStatement().setString(9, keySeed);
			weTopUpDS.getPreparedStatement().setString(10, keySeed);
			weTopUpDS.getPreparedStatement().setString(11, keySeed);
			weTopUpDS.getPreparedStatement().setString(12, jsonDecoder.getJsonObject().getString("distributor_id"));
			weTopUpDS.getPreparedStatement().setString(13, jsonDecoder.getJsonObject().getString("address"));
			weTopUpDS.getPreparedStatement().setString(14, jsonDecoder.getJsonObject().getString("dp_img"));
			weTopUpDS.getPreparedStatement().setString(15, jsonDecoder.getJsonObject().getString("doc_img_01"));
			weTopUpDS.getPreparedStatement().setString(16, jsonDecoder.getJsonObject().getString("doc_img_02"));
			weTopUpDS.getPreparedStatement().setString(17, jsonDecoder.getJsonObject().getString("doc_img_03"));
			boolean insertSuccess=false;
			try{ 
				weTopUpDS.execute();
				insertSuccess=true;
				
				userId=getUserId();
				if(!userId.equalsIgnoreCase("-1")) { //not -1 means user created successfully
					//insertToCustomerBalanceTable(userId);
					insertToTblChargingTable(userId,rate);

					errorCode = "0";
					errorMessage = "Successfully registered retailer.";
				}else {
					LogWriter.LOGGER.info("retailer insert/create failed");

					errorCode = "-1";
					errorMessage = "failed to register retailer.";
				}
			}catch(SQLIntegrityConstraintViolationException de) {
				errorCode = "1";
				errorMessage = "User with the email address or phone number exists.";
				LogWriter.LOGGER.info("SQLIntegrityConstraintViolationException:"+de.getMessage());
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("rs:SIVE");
				this.logWriter.appendAdditionalInfo(errorCode);
			}catch(SQLException e) {
				errorCode = "11";
				errorMessage = "SQLException.";
				LogWriter.LOGGER.severe("SQLException"+e.getMessage());
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("rs:SE");
				this.logWriter.appendAdditionalInfo("RetailerReg.registerNewRetailer():"+e.getMessage());
			}
			LogWriter.LOGGER.info("RetailerID:"+userId);
			
		}catch(SQLException e){
			errorCode= "11";
			errorMessage = "SQLException.";
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("rs:SE11");
			this.logWriter.appendAdditionalInfo("RetailerReg.registerNewRetailer():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
//			e.printStackTrace();
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("rs:E-3");
			this.logWriter.appendAdditionalInfo("RetailerReg.registerNewRetailer():"+e.getMessage());
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		if(errorCode.equals("0")) {
			return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserInfo(jsonDecoder.getJsonObject().getString("email"));
		}
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder removeRetailer(String user_id, String phone) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		
		String sql = "UPDATE `transaction_log` SET trx_status =? and additional_info =? WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, user_id);
			weTopUpDS.getPreparedStatement().setString(2, phone);
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
	
	/**
	 * 
	 * @return
	 * @throws SQLException
	 */
	private String getUserId() throws SQLException {
		String retval="-1";
		ResultSet rs=weTopUpDS.getGeneratedKeys();
		if(rs.next()) {
			retval=rs.getString(1);
		}
		return retval;
	}
	@SuppressWarnings("unused")
	private String getUserIdFromSequence(WeTopUpDS weTopUpDS) throws SQLException {
		String retval="-1";
		String sqlSequence="SELECT LAST_INSERT_ID()";
		weTopUpDS.prepareStatement(sqlSequence);
		ResultSet rs=weTopUpDS.executeQuery();
		if(rs.next()) {
			retval=rs.getString(1);
		}
		weTopUpDS.closePreparedStatement();		
		return retval;
	}

	private String updateCardListUser(String user_id, String trx_id) {
		return new UserDBOperations(this.weTopUpDS,this.configurations,this.logWriter).updateCardListUser(user_id,trx_id).getJsonObject().toString();
	}
	
}
