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

	// private static final Logger LOGGER =
	// LogWriter.LOGGER.getLogger(UserDBOperations.class.getName());
	/**
	 * 
	 */
	public UserDBOperations(WeTopUpDS weTopUpDS,Configurations configurations) {
		this.weTopUpDS = weTopUpDS;
		this.configurations = configurations;
	}

	
	public JsonEncoder verifyAppUser(String appname, String password) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General Error";
		String appPass = this.configurations.getTopUpUsers().containsKey(appname)? this.configurations.getTopUpUsers().get(appname):"";
		
		if(password.equals(appPass) && appPass!="") {
			errorCode="0";
			errorMessage = "Application authentication successful.";
		}
		else {
			errorCode="-5";
			errorMessage = "User is not authorized to perform this action.";
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

		public JsonEncoder insertTransaction(String user_id,String operator,String opType,String payee_name,String payee_phone,String payee_email,String amount,String trx_id,String remarks) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String errorCode = "-1";
		String errorMessage = "General Error";
		
		try {
			String sqlTransactionLog = "INSERT INTO transaction_log (user_id,operator,opType,payee_name,payee_phone,payee_email,amount,trx_id,remarks) " + "VALUES (?,?,?,?,?,?,?,?,?)";

			// params
			//"user_id":"2441139","operator":"Airtel","opType":"0","payee_name":"shaker","payee_phone":"+8801751501178","payee_email":"shaker@spiderdxb.com","amount":"100","trx_id":"TRX2441139","remarks":"this is just a test"
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
				weTopUpDS.getPreparedStatement().setString(9, remarks);

				weTopUpDS.execute();

				errorCode = "0";
				errorMessage = "successfully inserted into transaction_log";
				
				//LogWriter.LOGGER.info("inserted into transaction_log");

			} catch (SQLIntegrityConstraintViolationException de) {
				errorCode = "1";// : Same name Already exists
				errorMessage = "Same TRXID Already exists";
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
		jsonEncoder.buildJsonObject();
		// errorCode=jsonEncoder;

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
		
		public JsonEncoder getTransactionStatus(String trx_id) {
			JsonEncoder jsonEncoder = new JsonEncoder();
			String trx_status = "-99";
			String top_up_status = "-99";
			String errorCode = "-1";
			String errorMessage = "General error.";
			
			String sql = "SELECT trx_status, top_up_status FROM `transaction_log` WHERE trx_id=?";

			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, trx_id);
				weTopUpDS.executeQuery();
				if (weTopUpDS.getResultSet().next()) {
					trx_status = weTopUpDS.getResultSet().getString(1);
					top_up_status = weTopUpDS.getResultSet().getString(2);
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
			jsonEncoder.buildJsonObject();
			return jsonEncoder;
		}
}