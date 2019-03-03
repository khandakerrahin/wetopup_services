package org.spider.topupservices.Engine;

import org.spider.topupservices.DBOperations.UserRegistration;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Initializations.Configurations;

/**
 * @author hafiz
 *
 */
public class RegistrationProcessor {
	WeTopUpDS weTopUpDS;
	LogWriter logWriter;
	Configurations configurations;
	/**
	 * 
	 */
	public RegistrationProcessor(WeTopUpDS weTopUpDS,LogWriter logWriter, Configurations configurations) {
		// TODO Auto-generated constructor stub
		this.weTopUpDS=weTopUpDS;
		this.logWriter=logWriter;
		this.configurations=configurations;
	}
	
	/**
	 * @json user_name, phone, user_email, user_type, password
	 * @example {"appname":"value","apppass":"value","username":"Shaker","email":"shaker@spiderdxb.com","phone":"01751501178","password":"shaker123"}
	 * @requestParameters action=register&message=jsonDecoder
	 * @param message
	 * @param messageBody
	 * @return 0:Successfully Inserted
	 * <br>1:User with the email address or phone number exists
	 * <br>2:Inserting organization details failed
	 * <br>11:Inserting user credentials failed
	 * <br>E:JSON string invalid
	 * <br>-1:Default Error Code
	 * <br>-2:SQLException
	 * <br>-3:General Exception
	 * <br>-4:SQLException while closing connection
	 */
	public String processUserRegistration(String message, String messageBody) {
		String retval="E";
		JsonDecoder registrationInfo;
		if(messageBody.isEmpty()) {
			registrationInfo=new JsonDecoder(message);
		}else {
			registrationInfo=new JsonDecoder(messageBody);
		}
		if(registrationInfo.getErrorCode().equals("0")) {
			retval=new UserRegistration(this.weTopUpDS,this.logWriter,this.configurations).registerNewUser(registrationInfo).getJsonObject().toString();
		}else{
			//error decoding json
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	/**
	 * @json user_name, phone, user_email, user_type, password
	 * @example {"appname":"value","apppass":"value","username":"Shaker","email":"shaker@spiderdxb.com","phone":"01751501178","password":"shaker123"}
	 * @requestParameters action=register&message=jsonDecoder
	 * @param message
	 * @param messageBody
	 * @return 0:Successfully Inserted
	 * <br>1:User with the email address or phone number exists
	 * <br>2:Inserting organization details failed
	 * <br>11:Inserting user credentials failed
	 * <br>E:JSON string invalid
	 * <br>-1:Default Error Code
	 * <br>-2:SQLException
	 * <br>-3:General Exception
	 * <br>-4:SQLException while closing connection
	 */
	public String processRetailerRegistration(String message, String messageBody) {
		String retval="E";
		JsonDecoder registrationInfo;
		if(messageBody.isEmpty()) {
			registrationInfo=new JsonDecoder(message);
		}else {
			registrationInfo=new JsonDecoder(messageBody);
		}
		if(registrationInfo.getErrorCode().equals("0")) {
			retval=new UserRegistration(this.weTopUpDS,this.logWriter,this.configurations).registerNewRetailer(registrationInfo).getJsonObject().toString();
		}else{
			//error decoding json
			retval="E:JSON string invalid";
		}
		return retval;
	}
	
	public String removeRetailer(String message, String messageBody) {
		String retval="E";
		JsonDecoder registrationInfo;
		if(messageBody.isEmpty()) {
			registrationInfo=new JsonDecoder(message);
		}else {
			registrationInfo=new JsonDecoder(messageBody);
		}
		if(registrationInfo.getErrorCode().equals("0")) {
			retval=new UserRegistration(this.weTopUpDS,this.logWriter,this.configurations).removeRetailer(registrationInfo.getNString("user_id"),registrationInfo.getNString("phone")).getJsonObject().toString();
		}else{
			//error decoding json
			retval="E:JSON string invalid";
		}
		return retval;
	}
}
