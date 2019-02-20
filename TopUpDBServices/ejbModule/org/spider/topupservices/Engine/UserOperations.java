/**
 * 
 */
package org.spider.topupservices.Engine;

import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.DBOperations.UserDBOperations;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;

/**
 * @author hafiz
 *
 */
public class UserOperations {
	
	private LogWriter logWriter;
	private WeTopUpDS weTopUpDS;
	private Configurations configurations;
	
	public UserOperations(WeTopUpDS weTopUpDS, LogWriter logWriter, Configurations configurations) {
		this.weTopUpDS=weTopUpDS;
		this.logWriter=logWriter;
		this.configurations=configurations;
	}
	
	public String verify(String appname, String apppass) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).verifyAppUser(appname, apppass).getJsonObject().toString();
	}
	
	public String insertTrx(String user_id,String operator,String opType,String payee_name,String payee_phone,String payee_email,String amount,String trx_id,String remarks,String pay_method,String test) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).insertTransaction(user_id,operator,opType,payee_name,payee_phone,payee_email,amount,trx_id,remarks,pay_method,test).getJsonObject().toString();
	}
	
	public String sendEmail(String reset,String key,String email,String trx_id) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail(reset,key,email,trx_id).getJsonObject().toString();
	}
	
	public String updatePaymentMethod(String trx_id, String payment_method) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).updatePaymentMethod(trx_id, payment_method).getJsonObject().toString();
	}
	
	public String updateStatus(String trx_id, String status) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).updateTransactionStatus(trx_id, status).getJsonObject().toString();
	}
	
	public String getStatus(String trx_id) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getTransactionStatus(trx_id).getJsonObject().toString();
	}
	
	public String fetchSingleTransaction(String trx_id) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getSingleTransaction(trx_id).getJsonObject().toString();
	}
	
	public String getAccessKey(String trx_id,String test) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getAccessKey(trx_id,test).getJsonObject().toString();
	}
	
	public String fetchTrxHistory(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchTrxHistory(userID).getJsonObject().toString();
	}
	
	public String getTrxId(String message, String messageBody) {
		String retval = "N/A";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = json.getNString("trx_id");
		}
		return retval;
	}
	
	public String verifyUser(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = verify(json.getNString("appname"), json.getNString("apppass"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String insertTransaction(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		
		if (json.getErrorCode().equals("0")) {
			retval = insertTrx(
				//	NullPointerExceptionHandler.isNullOrEmpty(json.getNString("user_id"))?"":json.getNString("user_id"),
				json.getNString("user_id"),
				json.getNString("operator"),
				json.getNString("opType"),
				json.getNString("payee_name"),
				json.getNString("payee_phone"),
				json.getNString("payee_email"),
				json.getNString("amount"),
				json.getNString("trx_id"),
				json.getNString("remarks"),
				json.getNString("pay_method"),
				json.getNString("test")
			);
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String sendEmail(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		
		if (json.getErrorCode().equals("0")) {
			retval = sendEmail(json.getNString("reset"),json.getNString("key"),json.getNString("email"),json.getNString("trx_id"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String updatePaymentStatus(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = updateStatus(json.getNString("trx_id"), json.getNString("status"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String setPaymentMethod(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = updatePaymentMethod(json.getNString("trx_id"), json.getNString("payment_method"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String getTopUpStatus(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = getStatus(json.getNString("trx_id"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchSingleTransaction(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchSingleTransaction(json.getNString("trx_id"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchAccessKey(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = getAccessKey(json.getNString("trx_id"),json.getNString("test"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchTransactionHistory(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchTrxHistory(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
}
