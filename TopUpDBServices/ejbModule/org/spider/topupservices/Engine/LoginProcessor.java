package org.spider.topupservices.Engine;

import org.spider.topupservices.DBOperations.Login;
import org.spider.topupservices.DBOperations.UserInfo;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.Logs.LogWriter;

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
		if(new JsonDecoder(retval).getJsonObject().getString("ErrorCode").equals("0")) {
			retval=this.fetchUserInfo(loginCredentials.getJsonObject().getString("username"));
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
			retval=this.fetchUserEmail(loginCredentials.getJsonObject().getString("username"));
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
			retval=this.fetchUserByKey(loginCredentials.getJsonObject().getString("key"));
		}else{
			retval="E:JSON string invalid";
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
		return new UserInfo(this.weTopUpDS,this.logWriter).fetchUserInfo(id).getJsonObject().toString();
	}
	
	private String fetchUserEmail(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter).fetchUserEmail(id).getJsonObject().toString();
	}
	
	private String fetchUserByKey(String id) {
		return new UserInfo(this.weTopUpDS,this.logWriter).fetchUserByKey(id).getJsonObject().toString();
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
		this.logWriter.setUserId(loginCredentials.getJsonObject().getString("username"));
		return new Login(this.weTopUpDS,this.logWriter).compareCredentialsInDB(loginCredentials.getJsonObject().getString("username"),loginCredentials.getJsonObject().getString("password")).getJsonObject().toString();
	}
	
//	with MODE
//	public String checkCredentials(JsonDecoder loginCredentials){
//		this.logWriter.setUserId(loginCredentials.getJsonObject().getString("username"));
//		return new Login(this.weTopUpDS,this.logWriter).compareCredentialsInDB(loginCredentials.getJsonObject().getString("username"),loginCredentials.getJsonObject().getString("password"),Integer.parseInt(loginCredentials.getJsonObject().getString("mode"))).getJsonObject().toString();
//	}

	public String checkUserEntry(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.logWriter).checkUserInDB(loginCredentials.getJsonObject().getString("email"),loginCredentials.getJsonObject().getString("phone")).getJsonObject().toString();
	}
	
	public String checkUser(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.logWriter).checkUserInDB(loginCredentials.getJsonObject().getString("username")).getJsonObject().toString();
	}
	
	public String updateUserKey(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.logWriter).updateUserKey(loginCredentials.getJsonObject().getString("email"),loginCredentials.getJsonObject().getString("key")).getJsonObject().toString();
	}
	
	public String updateUserPassword(JsonDecoder loginCredentials){
		return new Login(this.weTopUpDS,this.logWriter).updateUserPassword(loginCredentials.getJsonObject().getString("key"),loginCredentials.getJsonObject().getString("password")).getJsonObject().toString();
	}
//	public String checkUser(JsonDecoder loginCredentials){
//		return new Login(this.weTopUpDS,this.logWriter).checkUserInDB(loginCredentials.getJsonObject().getString("username"),Integer.parseInt(loginCredentials.getJsonObject().getString("mode"))).getJsonObject().toString();
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
		return new Login(this.weTopUpDS,this.logWriter).compareCredentialsInDB(loginCredential,password,mode).getJsonObject().toString();
	}
	
}
