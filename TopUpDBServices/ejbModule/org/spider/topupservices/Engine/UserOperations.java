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
	
//	public String insertTopUpTrx(String user_id,String operator,String opType,String payee_name,String payee_phone,String payee_email,String amount,String trx_id,String remarks,String pay_method,String test) {
//		return new UserDBOperations(weTopUpDS,configurations,logWriter).insertTopUpTransaction(user_id,operator,opType,payee_name,payee_phone,payee_email,amount,trx_id,remarks,pay_method,test).getJsonObject().toString();
//	}
	
	
	public String insertTrx(String user_id,String amount,String trx_id,String trx_type,String operator,String opType,String payee_name,String payee_phone,String payee_email,String remarks,String test) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).insertTransaction(user_id,amount,trx_id,trx_type,operator,opType,payee_name,payee_phone,payee_email,remarks,test).getJsonObject().toString();
	}
	
	public String sendEmail(String action,String key,String email,String trx_id) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail(action,key,email,trx_id).getJsonObject().toString();
	}
	
	public String updatePaymentMethod(String trx_id, String payment_method) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).updatePaymentMethod(trx_id, payment_method).getJsonObject().toString();
	}
	
	public String updateStatus(String trx_id, String status) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).updateTopUpStatus(trx_id, status).getJsonObject().toString();
	}
	
	public String updatePayStatus(String trx_id, String status,String additional_info,String trx_type, String LP_trx_status,String payment_status) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).updateTransactionStatus(trx_id, status,additional_info,trx_type,LP_trx_status,payment_status).getJsonObject().toString();
	}
	
	public String getStatus(String trx_id) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getTopUpStatus(trx_id).getJsonObject().toString();
	}
	
	public String fetchSingleTransaction(String trx_id) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getSingleTransaction(trx_id).getJsonObject().toString();
	}
	
	public String getAccessKey(String trx_id,String test) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getAccessKey(trx_id,test).getJsonObject().toString();
	}
	
	public String fetchTrxHistory(String userID, String userType,String phone) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchTrxHistory(userID,userType,phone).getJsonObject().toString();
	}
	
	public String fetchRetailerList(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchRetailerList(userID).getJsonObject().toString();
	}
	
	public String fetchTopUpHistory(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchTopUpHistory(userID).getJsonObject().toString();
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
	
//	public String insertTopUpTransaction(String message, String messageBody) {
//		String retval = "E";
//		JsonDecoder json;
//		if (messageBody.isEmpty()) {
//			json = new JsonDecoder(message);
//		} else {
//			json = new JsonDecoder(messageBody);
//		}
//		
//		if (json.getErrorCode().equals("0")) {
//			retval = insertTopUpTrx(
//				//	NullPointerExceptionHandler.isNullOrEmpty(json.getNString("user_id"))?"":json.getNString("user_id"),
//				json.getNString("user_id"),
//				json.getNString("operator"),
//				json.getNString("opType"),
//				json.getNString("payee_name"),
//				json.getNString("payee_phone"),
//				json.getNString("payee_email"),
//				json.getNString("amount"),
//				json.getNString("trx_id"),
//				json.getNString("remarks"),
//				json.getNString("pay_method"),
//				json.getNString("test")
//			);
//		} else {
//			retval = "E:JSON string invalid";
//		}
//		return retval;
//	}
	
	public synchronized String insertTransaction(String message, String messageBody) {
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
				json.getNString("amount"),
				json.getNString("trx_id"),
				//	json.getNString("payment_method"),
				json.getNString("trx_type"),
				json.getNString("operator"),
				json.getNString("opType"),
				json.getNString("payee_name"),
				json.getNString("payee_phone"),
				json.getNString("payee_email"),
				json.getNString("remarks"),
				json.getNString("test")
				
				//	id, user_id, trx_id, amount, trx_status, insert_time, update_time, additional_info, payment_method, card_brand, card_number, bank, bkash_payment_number, billing_name, trx_type
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
			retval = sendEmail(json.getNString("action"),json.getNString("key"),json.getNString("email"),json.getNString("trx_id"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String updateTopUpStatus(String message, String messageBody) {
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
	
	public String updatePaymentStatus(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = updatePayStatus(json.getNString("trx_id"), json.getNString("status"), json.getNString("additional_info"),json.getNString("trx_type"),json.getNString("LP_trx_status"),json.getNString("payment_method"));
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
			retval = fetchTrxHistory(json.getNString("userID"),json.getNString("userType"),json.getNString("phone"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchRetailerList(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchRetailerList(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchTopUpHistory(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchTopUpHistory(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
}
