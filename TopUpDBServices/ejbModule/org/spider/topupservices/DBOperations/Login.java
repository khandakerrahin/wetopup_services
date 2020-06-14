package org.spider.topupservices.DBOperations;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Api.AuthenticationToken;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.DBOperations.UserDBOperations;
import org.spider.topupservices.Engine.GTokenVerifier;
import org.spider.topupservices.Engine.HttpsTrustManage;
import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Initializations.SecretKey;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;
import org.spider.topupservices.Utilities.RandomStringGenerator;

/**
 * @author hafiz
 *
 */
public class Login {
	WeTopUpDS weTopUpDS;
	LogWriter logWriter;
	Configurations configurations;
	/**
	 * 
	 */
	public Login(WeTopUpDS weTopUpDS, Configurations configurations, LogWriter logWriter) {
		this.weTopUpDS= weTopUpDS;
		this.logWriter=logWriter;
		this.configurations = configurations;
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
	 * @param loginCredential
	 * @param password
	 * @return
	 * 0:User verified
	 * 1:User credentials did not match
	 * E:General Exception
	 * -2:General Error at compareCredentialsInDB()
	 * -1: user not activated.
	 */
	public JsonEncoder compareCredentialsInDB(String loginCredential, String password, String pin, String gtoken, String userAuthToken) {
		int retval=-2;
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String email = "";
		String sql="select count(*) as counter, min(status) as status from users_info "
				+ "where (user_email=? or phone=?) and (passwd_enc=AES_ENCRYPT(?,concat_ws('',?,key_seed,key_seed,key_seed)) or (user_auth_token=? and user_auth_token IS NOT NULL and user_auth_token NOT IN ('')) or (pin IS NOT NULL and pin NOT IN ('') and pin=? and pin_retry_count<(SELECT config_value FROM configurations where id = 1)))";
		if(!NullPointerExceptionHandler.isNullOrEmpty(gtoken)) {
//			boolean isValid = new GTokenVerifier().verifyGToken(gtoken);
			
			boolean isValid = false;
			
			JsonDecoder jd;
			try {
				jd = new JsonDecoder(verifyGoogle(gtoken));
				if(jd.getEString("ErrorCode").equals("0")) {
					isValid = true;
					email = jd.getEString("email");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(isValid) {
				errorCode="0";
				errorMessage = "User is activated.";
				
				isValid = isUserEmailVerified(email);
				if(isValid) {
					LogWriter.LOGGER.info("User Gmail is verified.");
				} else {
					//	update user
					updateUser(email);
					LogWriter.LOGGER.info("User Gmail verification updated.");
				}
			} else {
				errorCode="-5";//default errorCode
				errorMessage = "User does not exist.";
			}
			
			jsonEncoder.addElement("ErrorCode", errorCode);
			jsonEncoder.addElement("ErrorMessage", errorMessage);
			jsonEncoder.buildJsonObject();
			
			return jsonEncoder;
		}
		try {
			int retryCount = getPinRetryCount(loginCredential);
			
			LogWriter.LOGGER.info("Pin retryCount : "+retryCount);
			
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, loginCredential);
			ps.setString(2, this.msisdnNormalize(loginCredential));
			ps.setString(3, NullPointerExceptionHandler.isNullOrEmpty(password)?"":password);
			ps.setString(4, SecretKey.SECRETKEY);
			ps.setString(5, NullPointerExceptionHandler.isNullOrEmpty(userAuthToken)?"":userAuthToken);
			ps.setString(6, NullPointerExceptionHandler.isNullOrEmpty(pin)?"":pin);
			ResultSet rs = ps.executeQuery();
			
			if(rs.next()) {
				retval=rs.getInt(1);
				LogWriter.LOGGER.info("User count:"+retval);
				if(retval>0) {
					errorCode="0";
					errorMessage = "User is activated.";
					resetPinRetryCount(this.msisdnNormalize(loginCredential));
				} else {
					if(retryCount>=5) {
						errorCode="6";
						errorMessage = "Maximum attempt of PIN match reached.";
					} else {
						errorCode="-5";//default errorCode
						errorMessage = "User does not exist.";
					}
					increasePinRetryCount(this.msisdnNormalize(loginCredential));
				}
				
			} else {
				errorCode="-5";//default errorCode
				errorMessage = "User does not exist.";
				increasePinRetryCount(this.msisdnNormalize(loginCredential));
			}
			rs.close();
			ps.close();			
		}catch(NullPointerException e) {
			increasePinRetryCount(this.msisdnNormalize(loginCredential));
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
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
	 * @return
	 * 0:User verified
	 * 1:User credentials did not match
	 * E:General Exception
	 * -2:General Error at compareCredentialsInDB()
	 * -1: user not activated.
	 */
	public JsonEncoder compareAdminCredentialsInDB(String loginCredential, String password, String pin, String gtoken) {
		String retval="-2";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "admin not found.";
		String email = "";
		String sql="select case when count(*)=0 then 1 else 0 end as counter, min(status) as status from admins_info "
				+ "where (user_email=? or phone=?) and (passwd_enc=AES_ENCRYPT(?,concat_ws('',?,key_seed,key_seed,key_seed))or pin=?)";
//		if(!NullPointerExceptionHandler.isNullOrEmpty(gtoken)) {
////			boolean isValid = new GTokenVerifier().verifyGToken(gtoken);
//			
//			boolean isValid = false;
//			
//			JsonDecoder jd;
//			try {
//				jd = new JsonDecoder(verifyGoogle(gtoken));
//				if(jd.getEString("ErrorCode").equals("0")) {
//					isValid = true;
//					email = jd.getEString("email");
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//			if(isValid) {
//				errorCode="0";
//				errorMessage = "admin is activated.";
//				
//				isValid = isUserEmailVerified(email);
//				if(isValid) {
//					LogWriter.LOGGER.info("Admin Gmail is verified.");
//				} else {
//					//	update user
//					updateUser(email);
//					LogWriter.LOGGER.info("User Gmail verification updated.");
//				}
//			} else {
//				errorCode="-5";//default errorCode
//				errorMessage = "User does not exist.";
//			}
//			
//			jsonEncoder.addElement("ErrorCode", errorCode);
//			jsonEncoder.addElement("ErrorMessage", errorMessage);
//			jsonEncoder.buildJsonObject();
//			
//			return jsonEncoder;
//		}
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, loginCredential);
			ps.setString(2, this.msisdnNormalize(loginCredential));
			ps.setString(3, NullPointerExceptionHandler.isNullOrEmpty(password)?"":password);
			ps.setString(4, SecretKey.ADMINSECRETKEY);
			ps.setString(5, NullPointerExceptionHandler.isNullOrEmpty(pin)?"":pin);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				retval=rs.getString(1);
				LogWriter.LOGGER.info("Admin count:"+retval);
				//this.logWriter.appendLog("uc:"+retval);
				if(!(rs.getString(2).equals("1"))) {
					retval="-1";
					errorCode="-1";//default errorCode
					errorMessage = "Admin is not activated.";
				}
				errorCode="0";
				errorMessage = "Admin is activated.";
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "Admin does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
			
			LogWriter.LOGGER.severe(e.getMessage());
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public String verifyGoogle(String gtoken) throws Exception {
		String urlReq = "https://10.10.1.13:8443/HttpReceiver/HttpReceiver?destinationName=spidertopupopcore&destinationType=queue&clientid=spidertopupopcore&target=ENGINE&LoadConf=N"
				+ "&reply=true&action=verifyGtoken";
		String data = "{\"appname\":\"WeTopUpRechargeServices\",\"apppass\":\"2441139&WE\",\"gtoken\":\"" + gtoken + "\"}";
		JsonObject personObject1;
		String response = "";

		try {
			HttpsTrustManage httptrustman = new HttpsTrustManage();
			httptrustman.TrustThyManager();
			URL url = new URL(urlReq);

			HttpURLConnection http = (HttpURLConnection) url.openConnection();
			http.setRequestProperty("Content-Type", "application/json; charset=utf8");
			http.setRequestMethod("POST");
			http.setDoOutput(true);

			// =============sending data===========
			DataOutputStream dos = new DataOutputStream(http.getOutputStream());
			dos.writeBytes(data);
			dos.flush();
			dos.close();
			String res_status = http.getResponseMessage();
			BufferedReader in = new BufferedReader(new InputStreamReader(http.getInputStream()));
			
			String inputLine;
			StringBuffer response1 = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				response1.append(inputLine);
			}
			in.close();
			JsonReader reader = Json.createReader(new StringReader(response1.toString()));
			personObject1 = reader.readObject();
			
			response = personObject1.toString();
			
			LogWriter.LOGGER.info("bean Response : " + personObject1.toString());
		} catch(Exception e) {
			
		}
		
		return response;

	}
	
	public JsonEncoder compareApiCredentialsInDB(String loginCredential, String password) {
		String retval="-2";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String sql="select case when count(*)=0 then 1 else 0 end as counter, min(status) as status from users_info where (user_email=? or phone=?) and api_key_enc=AES_ENCRYPT(?,concat_ws('',?,api_key_seed,api_key_seed,api_key_seed))";
		
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, loginCredential);
			loginCredential=this.msisdnNormalize(loginCredential);
			ps.setString(2, loginCredential);
			ps.setString(3, password);
			ps.setString(4, SecretKey.SECRETKEY);
			ResultSet rs = ps.executeQuery();
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
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
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
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, loginCredential);
			ps.setString(2, password);
			ps.setString(3, SecretKey.SECRETKEY);
			ResultSet rs = ps.executeQuery();
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
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder requestUserToken(String msisdn) {
		String userAuthToken = "";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(msisdn)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters";
		} else {
			JsonDecoder ckUser = new JsonDecoder(checkUserInDB(msisdn).getJsonObject().toString());
			
			
			if(ckUser.getEString("ErrorCode").equals("0")) {
				if(ckUser.getEString("userFlag").equals("0")) { //	user not found
				
					this.logWriter.appendLog("user not found.");
					
					JsonDecoder userInfo = new JsonDecoder("{\"phone\":\""+msisdn+"\"}") ;
					
					JsonDecoder regUser = new JsonDecoder(new UserRegistration(this.weTopUpDS,this.logWriter,this.configurations).registerNewAppUser(userInfo, "kotha").getJsonObject().toString());
					
					if(regUser.getEString("ErrorCode").equals("0")) {
						this.logWriter.appendLog("user registration successful.");
						
						userAuthToken = getUserAuthToken(msisdn);
						
						errorCode = "0";
						errorMessage = "Request Successful.";
					} else {
						this.logWriter.appendLog("user registration failed.");
						errorCode = "20";
						errorMessage = "User Registration failed";
					}
				}else {//	user found
					this.logWriter.appendLog("user found.");
					if(NullPointerExceptionHandler.isNullOrEmpty(ckUser.getEString("userAuthToken"))) {
						this.logWriter.appendLog("userAuthToken is null");
						userAuthToken = RandomStringGenerator.getRandomString("1234567890ABEDEFGHIJKLMNOPQRSTUVWXYZ.~$@*!-abcdefghijklmnopqrstuvwxyz",24);
						boolean updateFlag = updateUserAuthToken(msisdn, userAuthToken);
						
						if(!updateFlag) {
							userAuthToken = "";
							errorCode = "11";
							errorMessage = "Unknown Error";
						} else {
							errorCode = "0";
							errorMessage = "Request Successful.";
						}
						
					} else {
						userAuthToken = ckUser.getEString("userAuthToken");
						errorCode = "0";
						errorMessage = "Request Successful.";
					}
				}
				
			}
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userAuthToken", userAuthToken);
		
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder checkUserInDB(String loginCredential) {
		String userFlag="-2";
		String userStatus="-1";
		String userAuthToken = "";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String sql="select count(*) as counter, CASE WHEN max(status) IS NULL THEN 0 ELSE max(status) END AS status, CASE WHEN max(user_auth_token) IS NULL THEN '' ELSE max(user_auth_token) END AS userAuthToken from users_info where user_email=? or phone=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, loginCredential);
			ps.setString(2, this.msisdnNormalize(loginCredential));
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				userFlag=rs.getString(1);
				userStatus=rs.getString(2);
				userAuthToken=rs.getString(3);
				errorCode="0";
				errorMessage = "checked user successfully.";
				
				jsonEncoder.addElement("pinFlag", isPinSet(loginCredential)?"0":"5");
				jsonEncoder.addElement("userAuthToken", userAuthToken);
			}
			rs.close();
			ps.close();
			if(userStatus.equals("10")) {
				errorCode="50";
				errorMessage = "User is blocked.";
				this.logWriter.appendLog("user is blocked.");
			}
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
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
		String userStatus="-1";
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-2";//default errorCode
		String errorMessage = "user not found.";
		String sql="select count(*) as counter, CASE WHEN max(status) IS NULL THEN 0 ELSE max(status) END AS status from users_info where user_email=? or phone=?";
		
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, email);
			phone=this.msisdnNormalize(phone);
			ps.setString(2, phone);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				userFlag=rs.getString(1);
				userStatus=rs.getString(2);
				errorCode="0";
				errorMessage = "checked user successfully.";
			}
			rs.close();
			ps.close();
			
			if(userStatus.equals("10")) {
				errorCode="50";
				errorMessage = "User is blocked.";
			}
		}catch(NullPointerException e) {
			errorCode="-5";//default errorCode
			errorMessage = "User does not exist.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode="-3";//default errorCode
			errorMessage = "General Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userFlag", userFlag);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder requestOTP(String email) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String OTP = RandomStringGenerator.getRandomString("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", 6);
		

		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "UPDATE users_info t SET t.otp=?, t.last_sent_otp=NOW(), t.otp_expire = NOW() + interval 10 minute WHERE (t.user_email=?) and t.user_id > 0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, OTP);
			ps.setString(2, email);
			ps.execute();
			ps.close();
			errorCode = "0";
			errorMessage = "Update successful.";
			
			JsonDecoder json = new JsonDecoder(new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("sendOTP",  OTP,  email, null).getJsonObject().toString());
			
			errorCode = json.getNString("ErrorCode");
			errorCode = json.getNString("ErrorMessage");
			
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
	
	public JsonEncoder checkOTP(String email, String OTP) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		int count = 0;
		boolean flag = false;

		String sql = "select count(*) from users_info t where t.otp=? and t.otp_expire > NOW() and t.user_email=?";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, OTP);
			ps.setString(2, email);
			
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				count = rs.getInt(1);
			}
			if(count>0) {
				flag = updateUser(email, OTP);
				if(flag) {
					errorCode="0";
					errorMessage = "OTP matched successfully, user updated.";
				}else {
					errorCode="1";
					errorMessage = "OTP matched successfully, user update failed.";
				}
			} else {
				errorCode="5";
				errorMessage = "OTP match failed.";
			}
			rs.close();
			ps.close();			
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
	
	public JsonEncoder requestOTPV2(String email) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(email)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters for insertTransactionLog.";
		} else {
			//	check if already verified
			if(isUserEmailVerified(email)){
				errorCode = "7";
				errorMessage = "User email already verified.";
				jsonEncoder.addElement("status", "2");
			} else if(checkLastSentOTP(email)){
				errorCode = "8";
				errorMessage = "Multiple OTP request within a given interval.";
			} else {
				String sql = "UPDATE users_info t SET t.otp=?, t.last_sent_otp=NOW(), t.otp_expire = NOW() + interval 60 minute WHERE (t.user_email=?) and t.user_id > 0";
				String OTP = RandomStringGenerator.getRandomString("0123456789", 6);
				
				try {
					PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
					ps.setString(1, OTP);
					ps.setString(2, email);
					ps.execute();
					ps.close();
					errorCode = "0";
					errorMessage = "Update successful.";
					
					OTP = OTP + ","+ email;
					
					OTP = Base64.getEncoder().encodeToString(OTP.getBytes());
					JsonDecoder json = new JsonDecoder(new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("sendOTP",  OTP,  email, null).getJsonObject().toString());
					
					errorCode = json.getNString("ErrorCode");
					errorCode = json.getNString("ErrorMessage");
					
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
					errorCode = "11";
					errorMessage = "SQL Exception";
				}
			}
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder requestSmsOTP(String phone) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(phone)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters for insertTransactionLog.";
		} else {
			phone = msisdnNormalize(phone);
			//	check if already verified
//			if(isUserPhoneVerified(phone)){
//				errorCode = "7";
//				errorMessage = "User phone already verified.";
//				jsonEncoder.addElement("isPhoneVerified", "1");
//			} else if(checkLastSentOTP(phone)){
			if(checkLastSentOTP(phone)){
				errorCode = "8";
				errorMessage = "Multiple OTP request within a given interval.";
			} else {
				String sql = "UPDATE users_info t SET t.otp=?, t.otp_retry_count=?, t.last_sent_otp=NOW(), t.otp_expire = NOW() + interval 5 minute WHERE (t.phone=?) and t.user_id > 0";
				String OTP = RandomStringGenerator.getRandomString("0123456789", 4);
				
				try {
					PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
					ps.setString(1, OTP);
					ps.setInt(2, 0);
					ps.setString(3, phone);
					ps.execute();
					ps.close();
					errorCode = "0";
					errorMessage = "Update successful.";
					
					JsonDecoder json = new JsonDecoder(new UserDBOperations(weTopUpDS,configurations,logWriter).sendSms("sendOTP",  OTP,  phone, null));
					
					errorCode = json.getNString("ErrorCode");
					errorMessage = json.getNString("ErrorMessage");
					
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
					errorCode = "11";
					errorMessage = "SQL Exception";
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder checkOTPV2(String key) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		key = new String(Base64.getDecoder().decode(key)); 
		
		String val[] = key.split(",");
		
		if(val.length==2) {
			String OTP = val[0];
			String email = val[1];
			int count = 0;
			boolean flag = false;

			String sql = "select count(*) from users_info t where t.otp=? and t.otp_expire > NOW() and t.user_email=?";

			try {
				PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
				ps.setString(1, OTP);
				ps.setString(2, email);
				
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					count = rs.getInt(1);
				}
				if(count>0) {
					flag = updateUser(email, OTP);
					if(flag) {
						errorCode="0";
						errorMessage = "OTP matched successfully, user updated.";
					}else {
						errorCode="1";
						errorMessage = "OTP matched successfully, user update failed.";
					}
				} else {
					errorCode="5";
					errorMessage = "OTP match failed.";
				}
				rs.close();
				ps.close();			
			} catch (SQLException e) {
				LogWriter.LOGGER.severe(e.getMessage());
				errorCode = "11";
				errorMessage = "SQL Exception";
			}
		} else {
			errorCode="6";
			errorMessage = "Invalid OTP or email.";
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public boolean isPinSet(String creds) {
		boolean flag = false;
		String sql="select count(*) from users_info t where t.pin is not null and t.pin!='' and (t.phone=? or t.user_email =?)";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, this.msisdnNormalize(creds));
			ps.setString(2, creds);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if(rs.getInt(1)==1) {
					flag = true;
				}
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return flag;
	}
	
	public JsonEncoder checkPin(String pin, String creds) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		int count = 0;
		boolean flag = false;
		
		int retryCount = 0;
		//int retryCount = getOtpRetryCount(phone);
		// TODO later PIN retry count check
		
		LogWriter.LOGGER.info("Pin retryCount : "+retryCount);
		if(retryCount>=5) {
			errorCode="6";
			errorMessage = "Maximum attempt of PIN match reached.";
		}else {
			String sql = "select count(*) from users_info t where t.pin=? and (t.phone=? or t.user_email =?)";

			try {
				PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
				ps.setString(1, pin);
				ps.setString(2, this.msisdnNormalize(creds));
				ps.setString(3, creds);
				
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					count = rs.getInt(1);
				}
				if(count>0) {
				
					errorCode="0";
					errorMessage = "PIN matched successfully.";
				} else {
					errorCode="5";
					errorMessage = "PIN match failed.";
				}
				rs.close();
				ps.close();			
			} catch (SQLException e) {
				LogWriter.LOGGER.severe(e.getMessage());
				errorCode = "11";
				errorMessage = "SQL Exception";
			}
		}
		
		
		
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder checkSmsOTP(String OTP, String phone) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		int count = 0;
		boolean flag = false;
		phone = this.msisdnNormalize(phone);
		int retryCount = getOtpRetryCount(phone);
		LogWriter.LOGGER.info("OTP retryCount : "+retryCount);
		if(retryCount>=5) {
			errorCode="6";
			errorMessage = "Maximum attempt of OTP match reached.";
		}else {
			String sql = "select count(*) from users_info t where t.otp=? and t.otp_expire > NOW() and t.otp_retry_count<=5 and t.phone=?";

			try {
				PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
				ps.setString(1, OTP);
				ps.setString(2, phone);
				
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					count = rs.getInt(1);
				}
				if(count>0) {
//					flag = updatePhoneVerStatus(phone, OTP);
					flag = true;
					if(flag) {
						errorCode="0";
						errorMessage = "OTP matched successfully, user updated.";
					}else {
						errorCode="1";
						errorMessage = "OTP matched successfully, user update failed.";
					}
				} else {
					errorCode="5";
					errorMessage = "OTP match failed.";
					increaseOtpRetryCount(phone);
				}
				rs.close();
				ps.close();			
			} catch (SQLException e) {
				LogWriter.LOGGER.severe(e.getMessage());
				errorCode = "11";
				errorMessage = "SQL Exception";
			}
		}
		
		
		
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public int getPinRetryCount(String creds) {
		int count = 0;
		
		String sql="select t.pin_retry_count from users_info t where (t.phone=? or t.user_email=?)";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, this.msisdnNormalize(creds));
			ps.setString(2, creds);
			
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				count = rs.getInt(1);
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return count;
	}
	public int getOtpRetryCount(String phone) {
		int count = 0;
		
		String sql="select t.otp_retry_count from users_info t where t.phone=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, phone);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				count = rs.getInt(1);
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return count;
	}
	
	public boolean updateUserAuthToken(String msisdn, String authKey) {
		boolean flag = false;
		String sql = "UPDATE users_info t SET user_auth_token = ? WHERE t.phone=? and t.user_id > 0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, authKey);
			ps.setString(2, this.msisdnNormalize(msisdn));
			
			long updateCount = ps.executeUpdate();
			ps.close();
			
			if(updateCount>0) {
				this.logWriter.appendLog("userAuthToken Update Successful : " + msisdn + " " +authKey);
				flag = true;
			}else {
				this.logWriter.appendLog("userAuthToken Update Failed");
			}
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}
	
	public boolean updateUser(String email, String OTP) {
		boolean flag = false;
		String sql = "UPDATE users_info t SET status = ?, t.otp=null WHERE t.user_email=? and t.otp=? and t.user_id > 0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setInt(1, 2);	//	email verified
			ps.setString(2, email);
			ps.setString(3, OTP);
			
			long updateCount = ps.executeUpdate();
			ps.close();
			
			if(updateCount>0) {
				flag = true;
			}
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}
	
	public boolean updateUser(String email) {
		boolean flag = false;
		String sql = "UPDATE users_info t SET status = ?, t.otp=null WHERE t.user_email=? and t.user_id > 0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setInt(1, 2);	//	email verified
			ps.setString(2, email);
			
			long updateCount = ps.executeUpdate();
			ps.close();
			
			if(updateCount>0) {
				flag = true;
			}
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}
	
	public boolean updatePhoneVerStatus(String phone, String OTP) {
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		boolean flag = false;
		String sql = "UPDATE users_info t SET isPhoneVerified = ?, otp_retry_count=?, t.otp=null WHERE t.phone=? and t.otp=? and t.user_id > 0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setInt(1, 1);	//	phone verified
			ps.setInt(2, 0);	//	retry count reset
			ps.setString(3, phone);
			ps.setString(4, OTP);
			ps.execute();
			ps.close();
			errorCode = "0";
			errorMessage = "Update successful.";
			flag = true;
			
			//JsonDecoder json = new JsonDecoder(new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("userVerified",  OTP,  email, null).getJsonObject().toString());
			
//			errorCode = json.getNString("ErrorCode");
//			errorCode = json.getNString("ErrorMessage");
			
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			errorCode = "11";
			errorMessage = "SQL Exception";
		}

		return flag;
	}
	
	public boolean increasePinRetryCount(String cred) {
		
		boolean flag = false;
		String sql = "UPDATE users_info t SET pin_retry_count=pin_retry_count+1, pin_retry_unlock =case when (pin_retry_count+1)>=(SELECT config_value FROM configurations where id = 1) then (now() + interval 24 hour) " + 
				"else pin_retry_unlock end WHERE (t.phone=? or t.user_email=?) and t.user_id > 0 limit 1";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, cred);
			ps.setString(2, cred);
			
			ps.execute();
			ps.close();
			
			flag = true;
			LogWriter.LOGGER.info("PIN retry count increased.");
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		return flag;
	}
	
	public boolean resetPinRetryCount(String cred) {
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		boolean flag = false;
		String sql = "UPDATE users_info t SET t.pin_retry_count=?, t.pin_retry_unlock=null WHERE (t.phone=? or t.user_email=?) and t.user_id > 0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setInt(1, 0);	//	retry count reset
			ps.setString(2, cred);
			ps.setString(3, cred);
			ps.execute();
			ps.close();
			errorCode = "0";
			errorMessage = "Update successful.";
			
			flag = true;
			//JsonDecoder json = new JsonDecoder(new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("userVerified",  OTP,  email, null).getJsonObject().toString());
			
//			errorCode = json.getNString("ErrorCode");
//			errorCode = json.getNString("ErrorMessage");
			
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			errorCode = "11";
			errorMessage = "SQL Exception";
		}

		return flag;
	}
	
	public boolean increaseOtpRetryCount(String creds) {
		
		boolean flag = false;
		String sql = "UPDATE users_info t SET otp_retry_count=otp_retry_count+1, last_sent_otp =case when (otp_retry_count+1)=5 then (last_sent_otp + interval 20 minute) " + 
				"else last_sent_otp end WHERE (t.phone=? or t.user_email=?) and t.user_id > 0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, creds);
			ps.setString(2, creds);
			
			ps.execute();
			ps.close();
			
			flag = true;
			LogWriter.LOGGER.info("OTP retry count increased.");
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		return flag;
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
//			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
//			ps.setString(1, loginCredential);
//			ResultSet rs = ps.executeQuery();
//			while (rs.next()) {
//				userFlag=rs.getString(1);
//				errorCode="0";
//				errorMessage = "checked user successfully.";
//			}
//			rs.close();
//			ps.close();
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
//					rs.close();
//				} catch (SQLException e1) {
//					LogWriter.LOGGER.severe(e1.getMessage());
//				}
//				try {
//					ps.close();
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
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, key);
			ps.setString(2, email);
			ps.execute();
			ps.close();
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
	
	public JsonEncoder updateUserInfo(String email, String name, String address, String dp_img, String doc_img_01, String doc_img_02, String doc_img_03) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "update users_info set user_name=?, address=?, dp_img=?, doc_img_01=?, doc_img_02=?, doc_img_03=? where user_email=? and user_id>0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, name);
			ps.setString(2, address);
			ps.setString(3, dp_img);
			ps.setString(4, doc_img_01);
			ps.setString(5, doc_img_02);
			ps.setString(6, doc_img_03);
			ps.setString(7, email);
			ps.execute();
			ps.close();
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
	
	public JsonEncoder updateUserInfoByID(String userID, String email, String name, String phone, String address, String dp_img, String doc_img_01, String doc_img_02, String doc_img_03, String firebaseInstanceID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "update users_info set " + 
				"user_email = CASE WHEN ? = '' or ? IS NULL or (user_email IS NOT NULL and user_email!='') THEN user_email ELSE ? END,  " +
				"user_name = CASE WHEN ? = '' or ? IS NULL THEN user_name ELSE ? END, " + 
				"phone = CASE WHEN ? = '' or ? IS NULL or (phone IS NOT NULL and phone!='') THEN phone ELSE ? END, " + 
				"address = CASE WHEN ? = '' or ? IS NULL THEN address ELSE ? END," + 
				"dp_img = CASE WHEN ? = '' or ? IS NULL THEN dp_img ELSE ? END," + 
				"doc_img_01 = CASE WHEN ? = '' or ? IS NULL THEN doc_img_01 ELSE ? END," + 
				"doc_img_02 = CASE WHEN ? = '' or ? IS NULL THEN doc_img_02 ELSE ? END," + 
				"doc_img_03 = CASE WHEN ? = '' or ? IS NULL THEN doc_img_03 ELSE ? END, " +
				"firebase_instance_id = CASE WHEN ? = '' or ? IS NULL THEN firebase_instance_id ELSE ? END " +
				"where user_id  =?";

		String location = fetchUploadLocation();
		
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			
			ps.setString(1, email);
			ps.setString(2, email);
			ps.setString(3, email);
			
			ps.setString(4, name);
			ps.setString(5, name);
			ps.setString(6, name);
			
			ps.setString(7, this.msisdnNormalize(phone));
			ps.setString(8, this.msisdnNormalize(phone));
			ps.setString(9, this.msisdnNormalize(phone));
			
			ps.setString(10, address);
			ps.setString(11, address);
			ps.setString(12, address);
			
			ps.setString(13, dp_img);
			ps.setString(14, dp_img);
			ps.setString(15, location+dp_img);
			
			ps.setString(16, doc_img_01);
			ps.setString(17, doc_img_01);
			ps.setString(18, doc_img_01);
			
			ps.setString(19, doc_img_02);
			ps.setString(20, doc_img_02);
			ps.setString(21, doc_img_02);
			
			ps.setString(22, doc_img_03);
			ps.setString(23, doc_img_03);
			ps.setString(24, doc_img_03);
			
			ps.setString(25, firebaseInstanceID);
			ps.setString(26, firebaseInstanceID);
			ps.setString(27, firebaseInstanceID);
			
			ps.setInt(28, Integer.parseInt(userID));
			
			long updateCount = ps.executeUpdate();
			ps.close();
			
			if(updateCount>0) {
				errorCode = "0";
				errorMessage = "Update successful.";
				
				String uPhone = new UserDBOperations(this.weTopUpDS, this.configurations, this.logWriter).getUserPhone(userID);
				String uEmail = new UserDBOperations(this.weTopUpDS, this.configurations, this.logWriter).getUserEmail(userID);
				
				return new UserInfo(this.weTopUpDS,this.logWriter, this.configurations).fetchUserInfo(NullPointerExceptionHandler.isNullOrEmpty(uPhone)?uEmail:uPhone);
			}
			
		} catch (SQLIntegrityConstraintViolationException de) {
			errorCode = "1";// : Same name Already exists
			errorMessage = "SQLIntegrityConstraintViolationExceptions";
			LogWriter.LOGGER.severe("SQLIntegrityConstraintViolationException:" + de.getMessage());
		}catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			errorCode = "11";
			errorMessage = "SQL Exception";
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder requestUpdateUserInfo(String userID, String request) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "insert into user_update_requests (user_id,request_json) value (?,?)";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, userID);
			ps.setString(2, request);
			ps.execute();
			ps.close();
			errorCode = "0";
			errorMessage = "insert successful.";
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
	
	public JsonEncoder updateUserPin(String phone, String OTP, String pin, String oldPin) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		JsonDecoder jd;
		if(NullPointerExceptionHandler.isNullOrEmpty(phone) || NullPointerExceptionHandler.isNullOrEmpty(pin) || (NullPointerExceptionHandler.isNullOrEmpty(OTP) && NullPointerExceptionHandler.isNullOrEmpty(oldPin))) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			if(NullPointerExceptionHandler.isNullOrEmpty(OTP)) {
				jd = new JsonDecoder(checkPin(oldPin, phone).getJsonObject().toString());
			} else {
				jd = new JsonDecoder(checkSmsOTP(OTP, phone).getJsonObject().toString());
			}
			
			if(jd.getEString("ErrorCode").equals("0")) {
				String sql = "UPDATE users_info t SET t.pin=? WHERE t.phone=? and t.user_id > 0";

				phone=this.msisdnNormalize(phone);
				
				try {
					PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
					ps.setString(1, pin);
					ps.setString(2, phone);
					long updateCount = ps.executeUpdate();
					ps.close();
					if(updateCount>0) {
						errorCode = "0";
						errorMessage = "Update successful.";
						if(NullPointerExceptionHandler.isNullOrEmpty(OTP)) {
							// do nothing
						}else {
							boolean flag = updatePhoneVerStatus(phone, OTP);
							if(flag) {
								resetPinRetryCount(this.msisdnNormalize(phone));
								return new UserInfo(this.weTopUpDS,this.logWriter, this.configurations).fetchUserInfo(phone);
							}
							else {
								errorCode="1";
								errorMessage = "OTP matched successfully, user update failed.";
							}
						}
					} else {
						errorCode = "1";
						errorMessage = "Update Failed.";
					}
					
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
					errorCode = "11";
					errorMessage = "SQL Exception";
				}
			} else {
				errorCode = "-5";
				errorMessage = "OTP or pin match failed.";
			}
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
		String email = "";

		String keySeed = fetchkeySeedByKey(key);
		String sql = "UPDATE users_info t SET t.passwd_enc=AES_ENCRYPT(?,concat_ws('',?,?,?,?)) WHERE t.key=? and t.user_id > 0";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, password);
			ps.setString(2, SecretKey.SECRETKEY);//key
			ps.setString(3, keySeed);
			ps.setString(4, keySeed);
			ps.setString(5, keySeed);
			ps.setString(6, key);
			ps.execute();
			ps.close();
			errorCode = "0";
			errorMessage = "Update successful.";
			
			String userInfosql="SELECT u.user_id, u.user_name, case when u.user_email is null then '' else u.user_email end as user_email, u.user_type, u.phone, u.status FROM users_info u where u.key=?";

			try {
				ps = weTopUpDS.newPrepareStatement(userInfosql);
				ps.setString(1, key);
				
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					jsonEncoder.addElement("id", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_id"))?"":rs.getString("user_id"));
					jsonEncoder.addElement("username", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_name"))?"":rs.getString("user_name"));
					jsonEncoder.addElement("email", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_email"))?"":rs.getString("user_email"));
					jsonEncoder.addElement("phoneNumber", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("phone"))?"":rs.getString("phone"));
					jsonEncoder.addElement("userType", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_type"))?"":rs.getString("user_type"));
					jsonEncoder.addElement("status", NullPointerExceptionHandler.isNullOrEmpty(rs.getString("status"))?"0":rs.getString("status"));
					
					
					errorCode="0";
					errorMessage = "update and fetch successful.";
					email = NullPointerExceptionHandler.isNullOrEmpty(rs.getString("user_email"))?"":rs.getString("user_email");
					
					if(!email.equals("")) {
						boolean isValid = isUserEmailVerified(email);
						if(isValid) {
							LogWriter.LOGGER.info("User is verified.");
						} else {
							//	update user
//							updateUser(email);
							LogWriter.LOGGER.info("User email verification not updated.");
						}
					}
					
					
					this.logWriter.setStatus(1);
					this.logWriter.appendLog("fu:S");
				}else {
					errorCode="-9"; //User details could not be retrieved
					errorMessage = "User details could not be retrieved.";
					this.logWriter.setStatus(0);
					this.logWriter.appendLog("fu:F");
				}
				rs.close();
				ps.close();
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
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, newPassword);
			ps.setString(2, SecretKey.SECRETKEY);//key
			ps.setString(3, keySeed);
			ps.setString(4, keySeed);
			ps.setString(5, keySeed);
			ps.setString(6, credential);
			credential=this.msisdnNormalize(credential);
			ps.setString(7, credential);
			ps.execute();
			ps.close();
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
	
	public JsonEncoder changeAdminPassword(String credential, String newPassword) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		credential=this.msisdnNormalize(credential);
		
		String keySeed = fetchkeySeedByAdmin(credential);
		LogWriter.LOGGER.info("admin keySeed : "+keySeed);
		
		String sql = "UPDATE admins_info t SET t.passwd_enc=AES_ENCRYPT(?,concat_ws('',?,?,?,?)) WHERE t.user_email=? or t.phone=?";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, newPassword);
			ps.setString(2, SecretKey.ADMINSECRETKEY);//key
			ps.setString(3, keySeed);
			ps.setString(4, keySeed);
			ps.setString(5, keySeed);
			ps.setString(6, credential);
			credential=this.msisdnNormalize(credential);
			ps.setString(7, credential);
			ps.execute();
			ps.close();
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
	
	public JsonEncoder updateApiKey(String user_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		String apikey = RandomStringGenerator.getRandomString("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", 32);

		String apiKeySeed = fetchApikeySeedByUser(user_id);
		
		LogWriter.LOGGER.info("apiKeySeed : "+apiKeySeed);
		String sql = "UPDATE users_info t SET t.api_key_enc=AES_ENCRYPT(?,concat_ws('',?,?,?,?)) WHERE t.user_id =?";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, apikey);
			ps.setString(2, SecretKey.SECRETKEY);//key
			ps.setString(3, apiKeySeed);
			ps.setString(4, apiKeySeed);
			ps.setString(5, apiKeySeed);
			ps.setString(6, user_id);
			ps.execute();
			ps.close();
			errorCode = "0";
			errorMessage = "Update successful.";
			jsonEncoder.addElement("api_key", apikey);
			
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
	
	public JsonEncoder updateTokenValidity(String user_id,String validity) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		String sql = "UPDATE users_info t SET api_token_validity=? WHERE t.user_id =?";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, validity);
			ps.setString(2, user_id);
			ps.execute();
			ps.close();
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
	
	public JsonEncoder updateApiHookUrl(String user_id,String api_hook_url) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		
		String sql = "UPDATE users_info t SET api_hook_url=? WHERE t.user_id =?";

		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, api_hook_url);
			ps.setString(2, user_id);
			ps.execute();
			ps.close();
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
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, key);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				keySeed = rs.getString(1);
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return keySeed;
	}
	
	public String fetchUploadLocation() {
		String location = "";
		String sql="SELECT location FROM file_location where id=3";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				location = rs.getString(1);
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return location;
	}
	
	public boolean isUserEmailVerified(String email) {
		boolean flag = false;
		String sql="select status from users_info where `user_email`=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, email);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if(rs.getInt(1)==2) {
					flag = true;
				}
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return flag;
	}
	
	public boolean isUserBlocked(String userID) {
		boolean flag = true;
		String sql="select status from users_info where `user_id`=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, userID);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if(rs.getInt(1)==10) {
					flag = true;
				} else {
					flag = false;
				}
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			flag = true;
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
			flag = true;
		}
		
		return flag;
	}
	
	public boolean isUserPhoneVerified(String phone) {
		boolean flag = false;
		String sql="select isPhoneVerified from users_info where `phone`=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, phone);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if(rs.getInt(1)==1) {
					flag = true;
				}
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return flag;
	}
	
	public boolean checkLastSentOTP(String key) {
		boolean flag = false;
		String sql="select count(*) from users_info where (`user_email`=? or `phone`=?) and last_sent_otp >  NOW() - interval 1 minute";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, key);
			ps.setString(2, key);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if(rs.getInt(1)>0) {
					flag = true;
				}
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return flag;
	}
	
	public JsonEncoder fetchApiInfo(String user_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String apiKey = "";
		String api_token_validity = "";
		String api_hook_url = "";
		String errorCode = "-1";
		String errorMessage = "fetchApiInfo failed.";
		String sql="select ifnull(cast(aes_decrypt( api_key_enc,concat_ws('','$piderT@perEnc~ypt',api_key_seed,api_key_seed,api_key_seed)) as char(1000)),null),api_token_validity,api_hook_url from users_info where user_id = ?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, user_id);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				apiKey = rs.getString(1);
				api_token_validity = rs.getString(2);
				api_hook_url = rs.getString(3);
			}
			errorCode = "0";
			errorMessage = "fetchApiInfo successful.";
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			errorCode = "-5";
			errorMessage = "NullPointerException";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			errorCode = "-3";
			errorMessage = "GeneralException";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("api_key", NullPointerExceptionHandler.isNullOrEmpty(apiKey) ? "" : apiKey);
		jsonEncoder.addElement("api_token_validity", NullPointerExceptionHandler.isNullOrEmpty(api_token_validity) ? "" : api_token_validity);
		jsonEncoder.addElement("api_hook_url", NullPointerExceptionHandler.isNullOrEmpty(api_hook_url) ? "" : api_hook_url);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public String fetchkeySeedByUser(String credential) {
		String keySeed = "";
		String sql="select t.key_seed from users_info t where t.user_email=? or t.phone=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, credential);
			credential=this.msisdnNormalize(credential);
			ps.setString(2, credential);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				keySeed = rs.getString(1);
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return keySeed;
	}
	
	public String fetchkeySeedByAdmin(String credential) {
		String keySeed = "";
		String sql="select t.key_seed from admins_info t where t.user_email=? or t.phone=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, credential);
			credential=this.msisdnNormalize(credential);
			ps.setString(2, credential);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				keySeed = rs.getString(1);
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return keySeed;
	}
	
	public String fetchApikeySeedByUser(String credential) {
		String keySeed = "";
		String sql="select t.api_key_seed from users_info t where t.user_id=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, credential);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				keySeed = rs.getString(1);
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return keySeed;
	}
	
	public String getUserAuthToken(String phone) {
		String userAuthToken = "";
		String sql="select t.user_auth_token from users_info t where  t.phone=?";
		try {
			PreparedStatement ps = weTopUpDS.newPrepareStatement(sql);
			ps.setString(1, this.msisdnNormalize(phone));
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				userAuthToken = rs.getString(1);
			}
			rs.close();
			ps.close();
		}catch(NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		}
		catch(Exception e){
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return userAuthToken;
	}
}
