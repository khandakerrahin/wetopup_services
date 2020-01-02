package org.spider.topupservices.Engine;

import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Engine.JsonEncoder;

import java.util.Calendar;
import java.util.Date;

import org.spider.topupservices.Api.AuthenticationToken;
import org.spider.topupservices.DBOperations.Login;
import org.spider.topupservices.DBOperations.UserDBOperations;
import org.spider.topupservices.DBOperations.UserInfo;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;

//import org.spider.topupservices.Initializations.Configurations;

/**
 * @author hafiz
 * Get login data from webserver
 * match login data with db users table
 * 
 */
public class LoginProcessor {
	//	Login loginDBOperations;
	WeTopUpDS weTopUpDS;
	LogWriter logWriter;
	Configurations configurations;
	/**
	 * 
	 */

	public LoginProcessor(WeTopUpDS weTopUpDS,LogWriter logWriter, Configurations configurations) {
		//		loginDBOperations = new Login();
		this.weTopUpDS=weTopUpDS;
		this.logWriter=logWriter;
		this.configurations=configurations;
	}

	/**
	 * @json { "username":"t1@sp.com", "password":"specialt1pass", "mode":"1"} <br>mode 1:email, 2:phone
	 * @action login
	 * @param message jsonDecoder
	 * @param messageBody jsonDecoder
	 * @return jsonEncoder userInfo on success
	 * <br>errorCode 0 indicated success in fetching data
	 * <br>-1:General Error
	 * <br>-2:SQLException in fetchUserInfo()
	 * <br>-3:Exception
	 * <br>-4:SQLException while closing
	 * <br>0:User verified
	 * <br>1:User credentials did not match
	 * <br>E:General Exception
	 * <br>-2:General Error at compareCredentialsInDB()
	 * <br>E: General Error
	 * <br>E:JSON string invalid
	 * 
	 */
	public String processLogin(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkCredentials(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		if(new JsonDecoder(retval).getNString("ErrorCode").equals("0")) {
			retval=this.fetchUserInfo(loginCredentials.getNString("credential"));
			if(loginCredentials.isParameterPresent("trx_id")) {
				if(NullPointerExceptionHandler.isNullOrEmpty(loginCredentials.getNString("trx_id"))) {

				}else {
					this.updateCardListUser(new JsonDecoder(retval).getNString("user_id"),loginCredentials.getNString("trx_id"));
				}
			}
		}
		return retval;
	}
	
	public String processAdminLogin(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkAdminCredentials(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		if(new JsonDecoder(retval).getNString("ErrorCode").equals("0")) {
			retval=this.fetchAdminInfo(loginCredentials.getNString("credential"));
		}
		return retval;
	}
	
	public String fetchUser(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.fetchUserInfo(loginCredentials.getNString("credential"));
		}else{
			retval="E:JSON string invalid";
		}

		return retval;
	}

	public String authenticateUser(String message, String messageBody) {
		String retval="E";
		String respCode="0005";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkApiCredentials(loginCredentials);

			if(new JsonDecoder(retval).getNString("ErrorCode").equals("0")) {
				respCode="0000";
				retval=this.fetchUserInfoApi(loginCredentials.getNString("username"));
			}else{
				//retval="-6:Error in user Credentials";
				respCode="0010";			
			}
		}else{
			respCode="0009";
		}
		if(!respCode.equals("0000")) {
			JsonEncoder jsonEncoder=new JsonEncoder();
			jsonEncoder.addElement("ErrorCode", respCode);
			jsonEncoder.buildJsonObject();
			retval=jsonEncoder.getJsonObject().toString();
		}
		return retval;
	}

	public String getTopUpStatusApi(String message, String messageBody) {
		String retval="E";
		String respCode="0005";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkApiToken(loginCredentials.getNString("token"));
			JsonDecoder resp =new JsonDecoder(retval);
			if(resp.getNString("ErrorCode").equals("0000")) {
				// get topup status
				retval = this.fetchTopUpStatusApi(resp.getNString("userID"),loginCredentials.getNString("trx_id"),loginCredentials.getNString("user_trx_id"));
				respCode="0000";
			}else {
				respCode=resp.getNString("ErrorCode");
			}
		}else{
			respCode="0009";
		}
		if(!respCode.equals("0000")) {
			JsonEncoder jsonEncoder=new JsonEncoder();
			jsonEncoder.addElement("ErrorCode", respCode);
			jsonEncoder.buildJsonObject();
			retval=jsonEncoder.getJsonObject().toString();
		}
		return retval;
	}
	
	public String insertTopupApi(String message, String messageBody) {
		String retval="E";
		String respCode="0005";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkApiToken(loginCredentials.getNString("token"));
			JsonDecoder resp =new JsonDecoder(retval);
			if(resp.getNString("ErrorCode").equals("0000")) {
				// insert topup
				retval = this.insertTopupApi(resp.getNString("userID"),loginCredentials.getNString("phone"),loginCredentials.getNString("amount"),
						loginCredentials.getNString("op"),loginCredentials.getNString("isPostpaid"),loginCredentials.getNString("user_trx_id"));
				
				resp =new JsonDecoder(retval);
			}
			
			respCode=resp.getNString("ErrorCode");

		}else{
			respCode="0009";
		}
		if(!respCode.equals("0000")) {
			JsonEncoder jsonEncoder=new JsonEncoder();
			jsonEncoder.addElement("ErrorCode", respCode);
			jsonEncoder.buildJsonObject();
			retval=jsonEncoder.getJsonObject().toString();
		}
		return retval;
	}
	
	public String checkApiToken(String token) {
		JsonEncoder jsonEncoder=new JsonEncoder();
		LogWriter.LOGGER.info("token : "+token);
		String errorCode = "";
		if(NullPointerExceptionHandler.isNullOrEmpty(token)) {
			errorCode = "0003";
		} else {
			if(Configurations.authenticationTokenHM.containsKey(token)) {
				AuthenticationToken at= Configurations.authenticationTokenHM.get(token);
				String userID=at.getUserId();
				// Check validity		
				//LogWriter.LOGGER.info("validity date : "+at.getValidity().toString());
				if(checktokenValidity(at.getValidity())) {
					errorCode="0000";
					jsonEncoder.addElement("userID", userID);
				}else{
					if(Configurations.checkValidUserToken.containsKey(userID)) 
						Configurations.checkValidUserToken.remove(userID); 
		
					Configurations.authenticationTokenHM.remove(token);//remove token if validity is over
					errorCode="0004";//token expired
				}				
			}else {
				errorCode="0003";//token not found
			}
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.buildJsonObject();
		errorCode=jsonEncoder.getJsonObject().toString();
		return errorCode;
	}
	
	public boolean checktokenValidity(Date tokenValidity) {
		boolean retval=false;
		Calendar date = Calendar.getInstance();
		long t= date.getTimeInMillis();
		Date currentTime=new Date(t);
		//LogWriter.LOGGER.info("currentTime : "+currentTime.toString());
		if(currentTime.compareTo(tokenValidity)<0) {
			retval=true;
		}
		return retval;
	}
	
	public String fetchUserEmail(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.fetchUserEmail(loginCredentials.getEString("username"));
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String fetchUserByKey(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.fetchUserByKey(loginCredentials.getEString("key"));
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String fetchUserStatus(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.fetchUserStatus(loginCredentials.getEString("userID"));
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String fetchUserBalance(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.fetchUserBalance(loginCredentials.getNString("userID"));
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	/**
	 * 
	 * @param message
	 * @param messageBody
	 * @return
	 */
	public String fetchUserBalanceApi(String message, String messageBody) {


		String retval="0005";
		String respCode="0005";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		if(loginCredentials.getErrorCode().equals("0")) {
			if(NullPointerExceptionHandler.isNullOrEmpty(loginCredentials.getNString("token"))) {			
				retval=this.checkApiCredentials(loginCredentials);

				if(new JsonDecoder(retval).getNString("ErrorCode").equals("0")) {

					String username=loginCredentials.getEString("username");
					respCode="0000";				
					retval=this.fetchUserBalanceApi(username);
				}else{
					//retval="-6:Error in user Credentials";
					respCode="0010";			
				}
			}else{
				retval=this.checkApiToken(loginCredentials.getNString("token"));
				JsonDecoder resp =new JsonDecoder(retval);
				if(resp.getNString("ErrorCode").equals("0000")) {
					retval=this.fetchUserBalanceApiToken(resp.getNString("userID"));					
					resp =new JsonDecoder(retval);
				}
				respCode=resp.getNString("ErrorCode");
			}
		}else{
			respCode="0009";
		}


		if(!respCode.equals("0000")) {
			JsonEncoder jsonEncoder=new JsonEncoder();
			jsonEncoder.addElement("ErrorCode", respCode);
			jsonEncoder.buildJsonObject();
			retval=jsonEncoder.getJsonObject().toString();
		}
		return retval;
	}

	public String checkUser(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkUser(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String checkUserEntry(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkUserEntry(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String requestOTP(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.requestOTP(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	public String requestSmsOTP(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.requestSmsOTP(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String checkOTP(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkOTP(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	public String checkPin(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkPin(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	public String checkSmsOTP(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkSmsOTP(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String updateKey(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.updateUserKey(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String updateUserInfoByEmail(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.updateUserInfoByEmail(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String updateUserInfoByID(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.updateUserInfoByID(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	public String requestUpdateUserInfo(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.requestUpdateUserInfo(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String updatePassword(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.updateUserPassword(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	public String setPin(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.updateUserPin(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String updateApiKey(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.updateApiKey(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	public String updateTokenValidity(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.updateTokenValidity(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	public String updateApiHookUrl(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.updateApiHookUrl(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String fetchApiInfo(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}
		//LoginProcessor loginProcessor=new LoginProcessor();
		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.fetchApiInfo(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		return retval;
	}

	public String changePassword(String message, String messageBody) {
		String retval="E";
		JsonDecoder loginCredentials;
		if(messageBody.isEmpty()) {
			loginCredentials=new JsonDecoder(message);
		}else {
			loginCredentials=new JsonDecoder(messageBody);
		}

		if(loginCredentials.getErrorCode().equals("0")) {
			retval=this.checkCredentials(loginCredentials);
		}else{
			retval="E:JSON string invalid";
		}
		if(new JsonDecoder(retval).getEString("ErrorCode").equals("0")) {
			retval=this.changeUserPassword(loginCredentials);
		}
		return retval;
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
	 * 
	 */
	private String fetchUserInfo(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserInfo(id).getJsonObject().toString();
	}

	private String fetchAdminInfo(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchAdminInfo(id).getJsonObject().toString();
	}
	
	private String fetchUserInfoApi(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserInfoApi(id).getJsonObject().toString();
	}
	
	private String fetchTopUpStatusApi(String userID, String trx_id,String user_trx_id) {
		return new UserDBOperations(this.weTopUpDS,this.configurations,this.logWriter).fetchTopUpStatusApi(userID,trx_id,user_trx_id).getJsonObject().toString();
	}
	
	private String insertTopupApi(String userID, String phone, String amount, String op, String isPostpaid, String user_trx_id) {
		return new UserDBOperations(this.weTopUpDS,this.configurations,this.logWriter).insertTopupApi(userID, phone, amount, op, isPostpaid, user_trx_id).getJsonObject().toString();
	}
	
	private String updateCardListUser(String user_id, String trx_id) {
		return new UserDBOperations(this.weTopUpDS,this.configurations,this.logWriter).updateCardListUser(user_id,trx_id).getJsonObject().toString();
	}

	private String fetchUserEmail(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserEmail(id).getJsonObject().toString();
	}

	private String fetchUserByKey(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserByKey(id).getJsonObject().toString();
	}

	private String fetchUserStatus(String user_id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserStatus(user_id).getJsonObject().toString();
	}

	private String fetchUserBalance(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserBalance(id).getJsonObject().toString();
	}
	private String fetchUserBalanceApiToken(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserBalanceApiToken(id).getJsonObject().toString();
	}
	private String fetchUserBalanceApi(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter,this.configurations).fetchUserBalanceApi(id).getJsonObject().toString();
	}

	/**
	 * 
	 * @param loginCredentials
	 * @return 1:User verified
	 * 0:User credentials did not match
	 * E:General Exception
	 * -2:General Error at compareCredentialsInDB()
	 */
	public String checkCredentials(JsonDecoder loginCredentials){
		this.logWriter.setUserId(loginCredentials.getEString("credential"));
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).compareCredentialsInDB(loginCredentials.getEString("credential"),loginCredentials.getEString("password"),loginCredentials.getEString("pin"),loginCredentials.getEString("gtoken")).getJsonObject().toString();
	}

	public String checkAdminCredentials(JsonDecoder loginCredentials){
		this.logWriter.setUserId(loginCredentials.getEString("credential"));
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).compareAdminCredentialsInDB(loginCredentials.getEString("credential"),loginCredentials.getEString("password"),loginCredentials.getEString("pin"),loginCredentials.getEString("gtoken")).getJsonObject().toString();
	}
	
	public String checkApiCredentials(JsonDecoder loginCredentials){
		//this.logWriter.setUserId(loginCredentials.getEString("username"));
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).compareApiCredentialsInDB(loginCredentials.getEString("username"),loginCredentials.getEString("api_key")).getJsonObject().toString();
	}

	//	with MODE
	//	public String checkCredentials(JsonDecoder loginCredentials){
	//		this.logWriter.setUserId(loginCredentials.getEString("username"));
	//		return new Login(this.weTopUpDS,this.logWriter).compareCredentialsInDB(loginCredentials.getEString("username"),loginCredentials.getEString("password"),Integer.parseInt(loginCredentials.getEString("mode"))).getJsonObject().toString();
	//	}

	public String checkUserEntry(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).checkUserInDB(loginCredentials.getEString("email"),loginCredentials.getEString("phone")).getJsonObject().toString();
	}

	public String requestOTP(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).requestOTPV2(loginCredentials.getEString("email")).getJsonObject().toString();
	}
	
	public String requestSmsOTP(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).requestSmsOTP(loginCredentials.getEString("phone")).getJsonObject().toString();
	}

	public String checkOTP(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).checkOTPV2(loginCredentials.getEString("key")).getJsonObject().toString();
	}
	
	public String checkPin(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).checkPin(loginCredentials.getEString("pin"),loginCredentials.getEString("phone")).getJsonObject().toString();
	}
	
	public String checkSmsOTP(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).checkSmsOTP(loginCredentials.getEString("OTP"),loginCredentials.getEString("phone")).getJsonObject().toString();
	}

	public String checkUser(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).checkUserInDB(loginCredentials.getEString("username")).getJsonObject().toString();
	}

	public String updateUserKey(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).updateUserKey(loginCredentials.getEString("email"),loginCredentials.getEString("key")).getJsonObject().toString();
	}

	public String updateUserInfoByEmail(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).updateUserInfo(loginCredentials.getEString("email"),loginCredentials.getEString("name"),
				loginCredentials.getEString("address"), loginCredentials.getEString("dp_img"),loginCredentials.getEString("doc_img_01"),
				loginCredentials.getEString("doc_img_02"), loginCredentials.getEString("doc_img_03")).getJsonObject().toString();
	}
	
	public String updateUserInfoByID(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).updateUserInfoByID(loginCredentials.getEString("userID"),loginCredentials.getEString("email"),loginCredentials.getEString("name"),loginCredentials.getEString("phone"),
				loginCredentials.getEString("address"), loginCredentials.getEString("dp_img"),loginCredentials.getEString("doc_img_01"),
				loginCredentials.getEString("doc_img_02"), loginCredentials.getEString("doc_img_03"), loginCredentials.getEString("firebaseInstanceID")).getJsonObject().toString();
	}

	public String requestUpdateUserInfo(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).requestUpdateUserInfo(loginCredentials.getEString("userID"),loginCredentials.getJsonObject().toString()).getJsonObject().toString();
	}

	public String updateUserPassword(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).updateUserPassword(loginCredentials.getEString("key"),loginCredentials.getEString("password")).getJsonObject().toString();
	}

	public String updateUserPin(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).updateUserPin(loginCredentials.getEString("phone"),loginCredentials.getEString("OTP"),loginCredentials.getEString("pin"),loginCredentials.getEString("oldPin")).getJsonObject().toString();
	}
	
	public String changeUserPassword(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).changeUserPassword(loginCredentials.getEString("credential"),loginCredentials.getEString("newPassword")).getJsonObject().toString();
	}

	public String updateApiKey(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).updateApiKey(loginCredentials.getEString("user_id")).getJsonObject().toString();
	}
	
	public String updateTokenValidity(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).updateTokenValidity(loginCredentials.getEString("user_id"),loginCredentials.getEString("validity")).getJsonObject().toString();
	}
	
	public String updateApiHookUrl(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).updateApiHookUrl(loginCredentials.getEString("user_id"),loginCredentials.getEString("api_hook_url")).getJsonObject().toString();
	}
	
	public String fetchApiInfo(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).fetchApiInfo(loginCredentials.getEString("user_id")).getJsonObject().toString();
	}

	//	public String checkUser(JsonDecoder loginCredentials){
	//		return new Login(this.weTopUpDS,this.logWriter).checkUserInDB(loginCredentials.getEString("username"),Integer.parseInt(loginCredentials.getEString("mode"))).getJsonObject().toString();
	//	}


	/**
	 * 
	 * @param loginCredential
	 * @param password
	 * @param mode
	 * @return 1:User verified
	 * 0:User credentials did not match
	 * E:General Exception
	 * -2:General Error at compareCredentialsInDB()
	 */
	public String checkCredentials(String loginCredential, String password, int mode){
		return new Login(this.weTopUpDS,this.configurations,this.logWriter).compareCredentialsInDB(loginCredential,password,mode).getJsonObject().toString();
	}

}
