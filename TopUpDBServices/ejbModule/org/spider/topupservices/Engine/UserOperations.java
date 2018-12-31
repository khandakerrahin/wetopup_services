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
		return new UserDBOperations(weTopUpDS,configurations).verifyAppUser(appname, apppass).getJsonObject().toString();
	}
	
	public String insertTrx(String user_id,String operator,String opType,String payee_name,String payee_phone,String payee_email,String amount,String trx_id,String remarks) {
		return new UserDBOperations(weTopUpDS,configurations).insertTransaction(user_id,operator,opType,payee_name,payee_phone,payee_email,amount,trx_id,remarks).getJsonObject().toString();
	}
	
	public String updateStatus(String trx_id, String status) {
		return new UserDBOperations(weTopUpDS,configurations).updateTransactionStatus(trx_id, status).getJsonObject().toString();
	}
	
	public String getStatus(String trx_id) {
		return new UserDBOperations(weTopUpDS,configurations).getTransactionStatus(trx_id).getJsonObject().toString();
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
					json.getNString("user_id"),
					json.getNString("operator"),
					json.getNString("opType"),
					json.getNString("payee_name"),
					json.getNString("payee_phone"),
					json.getNString("payee_email"),
					json.getNString("amount"),
					json.getNString("trx_id"),
					json.getNString("remarks")
					);
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
}