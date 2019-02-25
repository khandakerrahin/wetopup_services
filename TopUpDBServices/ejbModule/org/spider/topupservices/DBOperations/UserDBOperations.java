/**
 * 
 */
package org.spider.topupservices.DBOperations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.nio.charset.Charset;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.Initializations.SecretKey;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;
import org.spider.topupservices.Utilities.RandomStringGenerator;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

/**
 * @author wasif update information delete user/organization
 */
public class UserDBOperations {
	WeTopUpDS weTopUpDS;
	Configurations configurations;
	LogWriter logWriter;

	// private static final Logger LOGGER =
	// LogWriter.LOGGER.getLogger(UserDBOperations.class.getName());
	/**
	 * 
	 */
	public UserDBOperations(WeTopUpDS weTopUpDS, Configurations configurations, LogWriter logWriter) {
		this.weTopUpDS = weTopUpDS;
		this.configurations = configurations;
		this.logWriter = logWriter;
	}

	public JsonEncoder verifyAppUser(String appname, String password) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General Error";

		String appPass = this.configurations.getTopUpUsers().containsKey(appname)
				? this.configurations.getTopUpUsers().get(appname)
				: "";

		if (password.equals(appPass) && appPass != "") {
			errorCode = "0";
			errorMessage = "Application authentication successful.";
		} else {
			errorCode = "-5";
			errorMessage = "User is not authorized to perform this action.";
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public String insertTopUpTransaction(String user_id, String operator, String opType, String payee_name,
			String payee_phone, String payee_email, String amount, String trx_id, String topup_trx_id, String remarks,String test) {
		
		String errorCode = "-1";
		String additional_info = null;
		
		if(NullPointerExceptionHandler.isNullOrEmpty(user_id) || NullPointerExceptionHandler.isNullOrEmpty(amount) 
				|| NullPointerExceptionHandler.isNullOrEmpty(trx_id) || NullPointerExceptionHandler.isNullOrEmpty(operator) || NullPointerExceptionHandler.isNullOrEmpty(opType) 
				|| NullPointerExceptionHandler.isNullOrEmpty(payee_phone) || NullPointerExceptionHandler.isNullOrEmpty(topup_trx_id)) {
			errorCode = "5";
		}else {
			try {
				String sqlTransactionLog = "INSERT INTO topup_log (user_id,operator,opType,payee_name,payee_phone,payee_email,amount,trx_id,topup_trx_id,remarks,additional_info) "
						+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

				// params
				// "user_id":"2441139","operator":"Airtel","opType":"0","payee_name":"shaker","payee_phone":"+8801751501178","payee_email":"shaker@spiderdxb.com","amount":"100","trx_id":"TRX2441139","remarks":"this
				// is just a test"
				try {
					weTopUpDS.prepareStatement(sqlTransactionLog, true);
					weTopUpDS.getPreparedStatement().setString(1, user_id);
					weTopUpDS.getPreparedStatement().setString(2, operator);
					weTopUpDS.getPreparedStatement().setString(3, opType);
					weTopUpDS.getPreparedStatement().setString(4, payee_name);
					weTopUpDS.getPreparedStatement().setString(5, payee_phone);
					weTopUpDS.getPreparedStatement().setString(6, payee_email);
					weTopUpDS.getPreparedStatement().setString(7, amount);
					weTopUpDS.getPreparedStatement().setString(8, trx_id);
					weTopUpDS.getPreparedStatement().setString(9, topup_trx_id);
					weTopUpDS.getPreparedStatement().setString(10, remarks);
					weTopUpDS.getPreparedStatement().setString(11, additional_info);

					weTopUpDS.execute();

					errorCode = "0";

				} catch (SQLIntegrityConstraintViolationException de) {
					errorCode = "1";// : Same name Already exists
					LogWriter.LOGGER.severe("SQLIntegrityConstraintViolationException:" + de.getMessage());
				} catch (SQLException e) {
					errorCode = "11";// :Inserting parameters failed
					e.printStackTrace();
					LogWriter.LOGGER.severe("SQLException" + e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
					errorCode = "10"; // :other Exception
					e.printStackTrace();
				}
			} finally {
				if (weTopUpDS.getConnection() != null) {
					try {
						weTopUpDS.closePreparedStatement();
						// weTopUpDS.getConnection().close();
					} catch (SQLException e) {
						errorCode = "-4"; // :connection close Exception
						e.printStackTrace();
						LogWriter.LOGGER.severe(e.getMessage());
					}
				}
			}
			// LogWriter.LOGGER.info("UserID:"+userId);

		}
		
				
		return errorCode;
	}
	
	public JsonEncoder insertTransaction(String user_id,String amount,String trx_id,String payment_method,String trx_type,String operator,String opType,String payee_name,String payee_phone,String payee_email,String remarks,String test) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String errorCode = "-1";
		String errorMessage = "General Error";
		String additional_info = null;
		String accessKey = "";
		String topup_trx_id = "";
		
		if(NullPointerExceptionHandler.isNullOrEmpty(user_id) || NullPointerExceptionHandler.isNullOrEmpty(amount) || NullPointerExceptionHandler.isNullOrEmpty(trx_id) || NullPointerExceptionHandler.isNullOrEmpty(payment_method) || NullPointerExceptionHandler.isNullOrEmpty(trx_type)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		}else {
			if(NullPointerExceptionHandler.isNullOrEmpty(operator)) {
				operator = "10"; // for balance
			}
			
			try {
				String sqlTransactionLog = "INSERT INTO transaction_log (user_id,amount,trx_id,payment_method,additional_info,trx_type) "
						+ "VALUES (?,?,?,?,?,?)";

				try {
					weTopUpDS.prepareStatement(sqlTransactionLog, true);
					weTopUpDS.getPreparedStatement().setString(1, user_id);
					weTopUpDS.getPreparedStatement().setString(2, amount);
					weTopUpDS.getPreparedStatement().setString(3, trx_id);
					weTopUpDS.getPreparedStatement().setString(4, payment_method);
					weTopUpDS.getPreparedStatement().setString(5, additional_info);
					weTopUpDS.getPreparedStatement().setString(6, trx_type);

					weTopUpDS.execute();

					errorCode = "0";
					errorMessage = "successfully inserted into transaction_log";

					LogWriter.LOGGER.info(errorMessage);
					
					if(trx_type.equals("0")) {
						topup_trx_id = RandomStringGenerator.getRandomString("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",10);
						errorCode = insertTopUpTransaction(user_id, operator, opType, payee_name,payee_phone, payee_email, amount, trx_id, topup_trx_id, remarks, test);
						if(errorCode.equals("0")) {
							errorMessage = "successfully inserted into topup_log";
							LogWriter.LOGGER.info(errorMessage);
						}else if(errorCode.equals("5")) {
							errorMessage = "Missing one or more parameters";
							LogWriter.LOGGER.info(errorMessage);
						}else {
							errorMessage = "failed to insert into topup_log";

							LogWriter.LOGGER.info(errorMessage);
						}
					}
					
					accessKey = fetchAccessKey(operator,test);
					
					LogWriter.LOGGER.info("accessKey : "+accessKey);

				} catch (SQLIntegrityConstraintViolationException de) {
					errorCode = "1";// : Same name Already exists
					errorMessage = "SQLIntegrityConstraintViolationExceptions";
					LogWriter.LOGGER.severe("SQLIntegrityConstraintViolationException:" + de.getMessage());
				} catch (SQLException e) {
					errorCode = "11";// :Inserting parameters failed
					errorMessage = "SQLException";
					e.printStackTrace();
					LogWriter.LOGGER.severe("SQLException" + e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
					errorCode = "10"; // :other Exception
					errorMessage = "other Exception";
					e.printStackTrace();
				}
			} finally {
				if (weTopUpDS.getConnection() != null) {
					try {
						weTopUpDS.closePreparedStatement();
						// weTopUpDS.getConnection().close();
					} catch (SQLException e) {
						errorCode = "-4"; // :connection close Exception
						errorMessage = "connection close Exception";
						e.printStackTrace();
						LogWriter.LOGGER.severe(e.getMessage());
					}
				}
			}
			// LogWriter.LOGGER.info("UserID:"+userId);

		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("accessKey", accessKey);
		
		jsonEncoder.buildJsonObject();
		// errorCode=jsonEncoder;

		return jsonEncoder;
	}
	
	public JsonEncoder sendEmail(String reset,String key,String email,String trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String errorCode = "-1";
		String errorMessage = "General Error";
		
		String app_name = "we_top_up";
		String subject =  "WeTopUp Recharge Service";
		String mailbody =  "WeTopUp Reset Password";
		String amount = "";
		String operator = "";
		String opType = "";
		String payee_phone = "";
		String time = "";
		String remarks = "";
		if(reset.equals("Y")) {
			subject =  "WeTopUp Reset Password";
			mailbody =  "Click this link to reset password. <a href='https://we-top-up.com/resetbymail.php?key="+key+"'>Link</a>";
		}
		else{
			JsonDecoder json =  new JsonDecoder(getTransactionStatus(trx_id).getJsonObject().toString());
			remarks = json.getNString("remarks");
			amount = json.getNString("amount");
			payee_phone = json.getNString("payee_phone");
			operator = getOperatorName(json.getNString("operator"));
			opType = getOpType(json.getNString("opType"));
			time = json.getNString("response_time");
			
			subject =  "WeTopUp Recharge Service";
			
			
			mailbody = "<b style='font-size: 20px;'> "+remarks+"</b> <br> <br>\n" + 
					"                <ul style='color:forestgreen'>\n" + 
					"                <li> <p style='color:green !important'> Recharge Details: BDT "+amount+" recharge in "+operator+" "+payee_phone+" ("+opType+") is successful. </p></li>\n" + 
					"                <li> <p style='color:black !important'> Date & Time: "+time+" </p></li>\n" + 
					"                <li> <p style='color:black !important'> Payment: "+amount+" Taka (no extra cost) via Lebupay </p></li>\n" + 
					"                <li> <p style='color:black !important'> Reference: "+trx_id+" </p></li>\n" + 
					"\n" + 
					"                <br>  <br> Thank you for allowing us to serve you <br>www.we-top-up.com <br> <img src='https://www.we-top-up.com/assets/images/icons/favicon-32x32.png' alt='Logo' title='Logo' style='display:block'> <br> <br> <div style='color: #707C80;font-size:10px'> Terms & Conditions:\n" + 
					"                    <br> 1. This is a Digital Invoice, which shall be treated as Delivery Challan of we-top-up service and does not required any Signature.\n" + 
					"                    <br> 2. We/SDC shall not accept any type of claims or complaints regarding the service, if the customer fails to contact us within 5 (Five) working days from the invoice date. In that case, it will be assumed that the customer has successfully received and enjoyed the service as per his/her satisfaction.\n" + 
					"                    <br> 3. Please contact if you have any kind of queries, complaints or claims regarding this Transaction:\n" + 
					"\n" + 
					"                        <br> E-mail: support@we-top-up.com\n" + 
					"                        <br> Tel: +880258157456\n" + 
					"                        <br> Facebook Messanger: m.me/weTopUp</div>";
		}
		
		
		String to_address =  email;
		String from_address = "WeTopUp <support@we-top-up.com>"; 
		String cc_address =  "";
		String bcc_address = "";
		
		try {
			String sqlTransactionLog = "INSERT INTO PostalServices.app_email_queues (app_name, subject, mailbody, to_address, from_address, cc_address, bcc_address) values (?, ?, ?, ?, ?, ?, ?)";

			try {
				weTopUpDS.prepareStatement(sqlTransactionLog, true);
				weTopUpDS.getPreparedStatement().setString(1, app_name);
				weTopUpDS.getPreparedStatement().setString(2, subject);
				weTopUpDS.getPreparedStatement().setString(3, mailbody);
				weTopUpDS.getPreparedStatement().setString(4, to_address);
				weTopUpDS.getPreparedStatement().setString(5, from_address);
				weTopUpDS.getPreparedStatement().setString(6, cc_address);
				weTopUpDS.getPreparedStatement().setString(7, bcc_address);

				weTopUpDS.execute();

				errorCode = "0";
				errorMessage = "successfully inserted into app_email_queue";


			} catch (SQLIntegrityConstraintViolationException de) {
				errorCode = "1";// : Same name Already exists
				errorMessage = "SQLIntegrityConstraintViolationExceptions";
				LogWriter.LOGGER.severe("SQLIntegrityConstraintViolationException:" + de.getMessage());
			} catch (SQLException e) {
				errorCode = "11";// :Inserting parameters failed
				errorMessage = "SQLException";
				e.printStackTrace();
				LogWriter.LOGGER.severe("SQLException" + e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				errorCode = "10"; // :other Exception
				errorMessage = "other Exception";
				e.printStackTrace();
			}
		} finally {
			if (weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closePreparedStatement();
					// weTopUpDS.getConnection().close();
				} catch (SQLException e) {
					errorCode = "-4"; // :connection close Exception
					errorMessage = "connection close Exception";
					e.printStackTrace();
					LogWriter.LOGGER.severe(e.getMessage());
				}
			}
		}
		// LogWriter.LOGGER.info("UserID:"+userId);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		//jsonEncoder.addElement("accessKey", accessKey);
		
		jsonEncoder.buildJsonObject();
		// errorCode=jsonEncoder;

		return jsonEncoder;
	}

	public String getOperatorName(String opCode){
		//	0=Airtel ; 1=Robi ; 2=GrameenPhone ; 3=Banglalink ; 4=Teletalk
		if(opCode.equals("0")){
			return "Airtel";
		}else if(opCode.equals("1")){
			return "Robi";
		}else if(opCode.equals("2")){
			return "GrameenPhone";
		}else if(opCode.equals("3")){
			return "Banglalink";
		}else if(opCode.equals("4")){
			return "Teletalk";
		}else{
			return "";
		}
	}
	
	public String getOpType(String opCode){
		//	1=Prepaid ; 2=Postpaid
		if(opCode.equals("1")){
			return "Prepaid";
		}else if(opCode.equals("2")){
			return "Postpaid";
		}else{
			return "";
		}
	}
	
	/**
	 * Only deletes from users table
	 * 
	 * @param userId
	 */
	public void deleteUsersEntry(String userId) {
		try {
			String sqlDeleteUser = "DELETE FROM users WHERE user_id=?";
			weTopUpDS.prepareStatement(sqlDeleteUser);
			weTopUpDS.getPreparedStatement().setString(1, userId);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			LogWriter.LOGGER.info("User entry deleted");
			this.logWriter.appendLog("dUe:rolledBack");
		} catch (SQLException e) {
			LogWriter.LOGGER.severe("deleteUsersEntry(): " + e.getMessage());
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("dUe:SE");
			this.logWriter.appendAdditionalInfo("UDO.deleteUsersEntry():" + e.getMessage());
		} finally {
			if (weTopUpDS.getConnection() != null) {
				try {
					if (!weTopUpDS.isPreparedStatementClosed())
						weTopUpDS.closePreparedStatement();
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
					this.logWriter.appendLog("s:SE");
					this.logWriter.appendAdditionalInfo("UDO.deleteUsersEntry():" + e.getMessage());
				}
			}
		}
	}

	public JsonEncoder updateTopUpStatus(String trx_id, String status) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "UPDATE `topup_log` SET trx_status =? WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, status);
			weTopUpDS.getPreparedStatement().setString(2, trx_id);
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
	
	public JsonEncoder updateTransactionStatus(String trx_id, String status) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "UPDATE `transaction_log` SET trx_status =? WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, status);
			weTopUpDS.getPreparedStatement().setString(2, trx_id);
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
	
	public JsonEncoder updateBalance(String user_id, String amount, String flag) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		Double currentBalance = 0.0;
		
		if(NullPointerExceptionHandler.isNullOrEmpty(user_id) || NullPointerExceptionHandler.isNullOrEmpty(amount) || NullPointerExceptionHandler.isNullOrEmpty(flag)) {
			errorCode = "-10";
			errorMessage = "Missing parameter(s).";
		}
		else {
			try {
				String sql = "UPDATE `user_balance` SET balance =? WHERE user_id=?";

				currentBalance = getCurrentBalance(user_id);
				if(flag.equals("0")) {	// ADD
					currentBalance = currentBalance + Double.parseDouble(amount);
					weTopUpDS.prepareStatement(sql);
					weTopUpDS.getPreparedStatement().setDouble(1, currentBalance);
					weTopUpDS.getPreparedStatement().setString(2, user_id);
					weTopUpDS.execute();
					weTopUpDS.closePreparedStatement();
					errorCode = "0";
					errorMessage = "Update successful.";
				}else if(flag.equals("1")) {	// SUB
					currentBalance = currentBalance - Double.parseDouble(amount);
					weTopUpDS.prepareStatement(sql);
					weTopUpDS.getPreparedStatement().setDouble(1, currentBalance);
					weTopUpDS.getPreparedStatement().setString(2, user_id);
					weTopUpDS.execute();
					weTopUpDS.closePreparedStatement();
					errorCode = "0";
					errorMessage = "Update successful.";
				}else {
					errorCode = "2";
					errorMessage = "Invalid flag.";
				}
			} catch (SQLException e) {
				LogWriter.LOGGER.severe(e.getMessage());
				errorCode = "11";
				errorMessage = "SQL Exception";
			}catch (Exception e) {
				LogWriter.LOGGER.severe(e.getMessage());
				errorCode = "-5";
				errorMessage = "General Exception";
			}
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("balance", ""+currentBalance);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public Double getCurrentBalance(String user_id) {
		Double balance = 0.0;
		String sql = "SELECT balance FROM user_balance where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, user_id);
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				balance = weTopUpDS.getResultSet().getDouble(1);
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		return balance;
	}
	
	public JsonEncoder updatePaymentMethod(String trx_id, String payment_method) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		if (NullPointerExceptionHandler.isNullOrEmpty(trx_id)) {
			errorCode = "5";
			errorMessage = "can not update for empty trx_id.";
		} else {
			String sql = "UPDATE `topup_log` SET payment_method =? WHERE trx_id=?";

			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, payment_method);
				weTopUpDS.getPreparedStatement().setString(2, trx_id);
				weTopUpDS.execute();
				weTopUpDS.closePreparedStatement();
				errorCode = "0";
				errorMessage = "Update successful.";
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

	public JsonEncoder getTransactionStatus(String trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String trx_status = "-99";
		String top_up_status = "-99";
		String user_id = "";
		String payee_phone = "";
		String amount = "";
		String operator = "";	
		String opType = "";
		String payee_email = "";
		String remarks = "";
		String response_time = "";
		
		String errorCode = "-1";
		String errorMessage = "General error.";

		String sql = "SELECT trx_status, top_up_status, user_id, payee_phone, amount, operator, opType, payee_email, remarks, response_time FROM `topup_log` WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, trx_id);
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				trx_status = weTopUpDS.getResultSet().getString(1);
				top_up_status = weTopUpDS.getResultSet().getString(2);
				user_id = weTopUpDS.getResultSet().getString(3);
				payee_phone = weTopUpDS.getResultSet().getString(4);
				amount = weTopUpDS.getResultSet().getString(5);
				operator = weTopUpDS.getResultSet().getString(6);	
				opType = weTopUpDS.getResultSet().getString(7);
				payee_email = weTopUpDS.getResultSet().getString(8);
				remarks = weTopUpDS.getResultSet().getString(9);
				response_time = weTopUpDS.getResultSet().getString(10);
				errorCode = "0";
				errorMessage = "getStatus successful.";
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("trx_id", trx_id);
		jsonEncoder.addElement("trx_status", trx_status);
		jsonEncoder.addElement("top_up_status", top_up_status);
		jsonEncoder.addElement("user_id", user_id);
		jsonEncoder.addElement("payee_phone", payee_phone);
		jsonEncoder.addElement("amount", amount);
		jsonEncoder.addElement("operator", operator);
		jsonEncoder.addElement("opType", opType);
		jsonEncoder.addElement("payee_email", payee_email);
		jsonEncoder.addElement("remarks", remarks);
		jsonEncoder.addElement("response_time", response_time);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public String fetchAccessKey(String operator, String test) {
		String accessKey = "";
		int accessFlag = 1;
		
		// Operator:  0=Airtel ; 1=Robi ; 2=GrameenPhone ; 3=Banglalink ; 4=Teletalk
		
		if(test.equals("Y")) {
			
		}else {
			if(operator.equals("0") || operator.equals("1")) {
				accessFlag = 2;
			}else if(operator.equals("10")) {
				accessFlag = 5;
			}else {
				accessFlag = 3;
			}
		}
		
		String sql = "SELECT access_key FROM lebupay_access_key where id = ?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, accessFlag);
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				accessKey = weTopUpDS.getResultSet().getString(1);
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return accessKey;
	}
	
	public JsonEncoder getSingleTransaction(String trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String user_id = "";
		String payee_phone="";
		String amount="";
		String operator="";
		String opType="";
		String payee_email="";
		String remarks="";
		String errorCode = "-1";
		String errorMessage = "General error.";

		String sql = "SELECT user_id, payee_phone, amount, operator, opType, payee_email, remarks FROM topup_log WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, trx_id);
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				user_id = weTopUpDS.getResultSet().getString(1);
				payee_phone = weTopUpDS.getResultSet().getString(2);
				amount = weTopUpDS.getResultSet().getString(3);
				operator = weTopUpDS.getResultSet().getString(4);
				opType = weTopUpDS.getResultSet().getString(5);
				payee_email = weTopUpDS.getResultSet().getString(6);
				remarks = weTopUpDS.getResultSet().getString(7);
				errorCode = "0";
				errorMessage = "getStatus successful.";
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("trx_id", trx_id);
		jsonEncoder.addElement("user_id", user_id);
		jsonEncoder.addElement("payee_phone", payee_phone);
		jsonEncoder.addElement("amount", amount);
		jsonEncoder.addElement("operator", operator);
		jsonEncoder.addElement("opType", opType);
		jsonEncoder.addElement("payee_email", payee_email);
		jsonEncoder.addElement("remarks", remarks);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getAccessKey(String trx_id,String test) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String operator="";
		String accessKey="";
		String errorCode = "-1";
		String errorMessage = "General error.";

		String sql = "SELECT operator FROM topup_log WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, trx_id);
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				operator = weTopUpDS.getResultSet().getString(1);
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
			
			accessKey = fetchAccessKey(operator, test);
			
			if(accessKey.equals("")) {
				errorCode = "5";
				errorMessage = "fetchAccessKey failed.";
			}else {
				errorCode = "0";
				errorMessage = "fetchAccessKey successful.";
			}
		
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("accessKey", accessKey);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder fetchTrxHistory(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String trx_history = "";

		String sql = "SELECT trx_id, amount, trx_status, insert_time FROM transaction_log where trx_type=? and user_id=? order by insert_time desc";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, "1"); // for balance recharge
			weTopUpDS.getPreparedStatement().setString(2, userID);
			weTopUpDS.executeQuery();
			while (weTopUpDS.getResultSet().next()) {
				// trx_id,payee_phone,amount,trx_status,top_up_status,insert_time,payment_method
				trx_history += "\"" + weTopUpDS.getResultSet().getString(1) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(2) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(3) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(4) + "\"" + "|";
			}
			int lio = trx_history.lastIndexOf("|");
			if (lio > 0)
				trx_history = trx_history.substring(0, lio);

			errorCode = "0";
			errorMessage = "fetched transaction history successfully.";

			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("trx_history :"+trx_history);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("trx_history", trx_history);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchTopUpHistory(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String trx_history = "";

		String sql = "SELECT a.trx_id, a.payee_phone, a.amount, t.trx_status, a.top_up_status, a.insert_time, t.payment_method FROM topup_log a left join transaction_log t on a.trx_id=t.trx_id WHERE a.user_id=? order by a.insert_time desc";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			weTopUpDS.executeQuery();
			while (weTopUpDS.getResultSet().next()) {
				// trx_id,payee_phone,amount,trx_status,top_up_status,insert_time,payment_method
				trx_history += "\"" + weTopUpDS.getResultSet().getString(1) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(2) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(3) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(4) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(5) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(6) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(7) + "\"" + "|";
			}
			int lio = trx_history.lastIndexOf("|");
			if (lio > 0)
				trx_history = trx_history.substring(0, lio);

			errorCode = "0";
			errorMessage = "fetched transaction history successfully.";

			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("trx_history :"+trx_history);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("trx_history", trx_history);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public double getBalance(String id) {
		// id = 1 = topUp
		// id = 2 = paywell
		double balance = 0.0;
		String sql = "SELECT retailer_balance FROM `balance_info` WHERE id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				balance = weTopUpDS.getResultSet().getDouble(1);
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return balance;
	}
}