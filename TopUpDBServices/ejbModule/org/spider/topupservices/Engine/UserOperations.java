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
	
	public String verify(String appname, String apppass, String appToken) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).verifyAppUser(appname, apppass, appToken).getJsonObject().toString();
	}
	
//	public String insertTopUpTrx(String user_id,String operator,String opType,String payee_name,String payee_phone,String payee_email,String amount,String trx_id,String remarks,String pay_method,String test) {
//		return new UserDBOperations(weTopUpDS,configurations,logWriter).insertTopUpTransaction(user_id,operator,opType,payee_name,payee_phone,payee_email,amount,trx_id,remarks,pay_method,test).getJsonObject().toString();
//	}
	
	
	public String insertTrx(String user_id,String amount,String trx_id,String user_trx_id,String src,String trx_type,String operator,String opType,String payee_name,String payee_phone,String payee_email,String remarks,String test,String ref_file_id, String overrideBalance) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).insertTransaction(user_id,amount,trx_id,user_trx_id,src,trx_type,operator,opType,payee_name,payee_phone,payee_email,remarks,test,ref_file_id,overrideBalance).getJsonObject().toString();
	}
	
	public String topupFileInserts(String user_id,String filename,String updated_filename) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).topupFileInsert(user_id,filename,updated_filename).getJsonObject().toString();
	}
	
	public String addQuickRecharge(String userID,String phone,String amount, String operator, String opType, String remarks) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).addQuickRecharge(userID, phone, amount,  operator,  opType,  remarks).getJsonObject().toString();
	}
	
	public String modifyQuickRecharge(String userID,String quickID, String phone,String amount, String operator, String opType, String remarks, String flag) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).modifyQuickRecharge(userID, quickID, phone, amount,  operator,  opType,  remarks, flag).getJsonObject().toString();
	}
	
	public String getQuickRecharge(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getQuickRecharge(userID).getJsonObject().toString();
	}
	
	public String fetchUserLimits(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getUserOperationConfig(userID).getJsonObject().toString();
	}
	
	public String getOpOffers(String operator,String flag, String lastUpdateTime) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getOffers(operator,flag, lastUpdateTime).getJsonObject().toString();
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
	
	public String updatePendingTrx(String card_number, String user_id, String admin_id, String status, String guest_status) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).updatePendingTrx(card_number, user_id, admin_id, status, guest_status).getJsonObject().toString();
	}
	
	public String requestPendingTrxUser(String trx_id, String jsonReq) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).requestPendingTrxUser(trx_id, jsonReq).getJsonObject().toString();
	}
	
	public String updatePayStatus(String trx_id, String status,String additional_info,String trx_type, String LP_trx_status,String payment_status, String card_brand, String card_number, String bank, String bkash_payment_number, String billing_name, String card_region, String binIssuerCountry, String binIssuerBank) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).updateTransactionStatus(trx_id, status,additional_info,trx_type,LP_trx_status,payment_status,card_brand, card_number, bank, bkash_payment_number, billing_name, card_region, binIssuerCountry, binIssuerBank).getJsonObject().toString();
	}
	
	public String getStatus(String userID, String trx_id, String user_trx_id) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getTopUpStatus(userID, trx_id, user_trx_id).getJsonObject().toString();
	}
	
	public String fetchUserRates(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchUserRates(userID).getJsonObject().toString();
	}
	
	public String getFileTopupSummary(String userID, String fileID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getFileTopupSummary(userID, fileID).getJsonObject().toString();
	}
	
	public String getFileTopupHistory(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getFileTopupHistory(userID).getJsonObject().toString();
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
	
	public String fetchStocksHistory(String userID, String startDate, String endDate) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchStocksHistory(userID, startDate, endDate).getJsonObject().toString();
	}
	public String fetchRetailerList(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchRetailerList(userID).getJsonObject().toString();
	}
	
	public String allocateShadowOpBalance(String userID, String operator, String amount) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).allocateShadowOpBalance(userID, operator, amount).getJsonObject().toString();
	}
	
	public String checkForUpdates(String userID, String appVersion) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).checkForUpdates(userID, appVersion).getJsonObject().toString();
	}
	
	public String fetchOpBalance() {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchOpBalance().getJsonObject().toString();
	}
	
	public String fetchShadowOpBalance() {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchShadowOpBalance().getJsonObject().toString();
	}
	
	public String fetchAllBalance(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchAllBalance(userID).getJsonObject().toString();
	}
	
	public String fetchUploadStatus(String user_id, String fileID, String extended) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).getFileUploadStatus(user_id, fileID, extended).getJsonObject().toString();
	}
	
	public String fetchDownloadStatus(String user_id, String downloadID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchDownloadStatus(user_id, downloadID).getJsonObject().toString();
	}
	
	public String fetchUserCardList(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchUserCardList(userID).getJsonObject().toString();
	}
	
	public String fetchPendingTrxUser(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchPendingTrxUser(userID).getJsonObject().toString();
	}
	
	public String fetchUserCardCounts(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchUserCardCounts(userID).getJsonObject().toString();
	}
	
	public String fetchPendingTrxAdmin(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchPendingTrxAdmin(userID).getJsonObject().toString();
	}
	
	public String fetchTopUpHistory(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchTopUpHistory(userID).getJsonObject().toString();
	}
	
	public String fetchShadowOpBalanceHistory(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchShadowOpBalanceHistory(userID).getJsonObject().toString();
	}
	
	public String fetchFileTopUpHistory(String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchFileTopUpHistory(userID).getJsonObject().toString();
	}
	
	public String fetchSingleFileTopUps(String userID,String fileID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchSingleFileTopUpHistory(userID,fileID).getJsonObject().toString();
	}
	
	public String fetchUploadedFileDetail(String fileID,String userID) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchUploadedFileDetails(fileID,userID).getJsonObject().toString();
	}
	
	public String downloadUploadedFileDetails(String fileID, String userID, String outputType, String reportType) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).downloadUploadedFileDetails(fileID, userID, outputType, reportType).getJsonObject().toString();
	}
	
	public String fetchInvalidFileRow(String fileID,String userID ) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).fetchInvalidFileRow(fileID, userID).getJsonObject().toString();
	}
	
	public String processFileTopups(String userID, String fileID, String processFlag) {
		return new UserDBOperations(weTopUpDS,configurations,logWriter).processFileTopups(userID, fileID,processFlag).getJsonObject().toString();
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
	
	public String verifyUser(String message, String messageBody, String appToken) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = verify(json.getNString("appname"), json.getNString("apppass"), appToken);
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
				json.getNString("user_trx_id"),
				json.getNString("src"),
				//	json.getNString("payment_method"),
				json.getNString("trx_type"),
				json.getNString("operator"),
				json.getNString("opType"),
				json.getNString("payee_name"),
				json.getNString("payee_phone"),
				json.getNString("payee_email"),
				json.getNString("remarks"),
				json.getNString("test"),
				json.getNString("ref_file_id"),
				json.getEString("overrideBalance")
				
				//	id, user_id, trx_id, amount, trx_status, insert_time, update_time, additional_info, payment_method, card_brand, card_number, bank, bkash_payment_number, billing_name, trx_type
			);
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String topupFileInsert(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		
		if (json.getErrorCode().equals("0")) {
			retval = topupFileInserts(
				json.getNString("user_id"),
				json.getNString("filename"),
				json.getNString("updated_filename")
			);
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String addQuickRecharge(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		
		if (json.getErrorCode().equals("0")) {
			retval = addQuickRecharge(
				json.getNString("userID"),
				json.getNString("phone"),
				json.getNString("amount"),
				json.getNString("operator"),
				json.getNString("opType"),
				json.getNString("remarks")
			);
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}

	public String modifyQuickRecharge(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		
		if (json.getErrorCode().equals("0")) {
			retval = modifyQuickRecharge(
				json.getNString("userID"),
				json.getNString("quickID"),
				json.getNString("phone"),
				json.getNString("amount"),
				json.getNString("operator"),
				json.getNString("opType"),
				json.getNString("remarks"),
				json.getNString("flag")
			);
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String getQuickRecharge(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		
		if (json.getErrorCode().equals("0")) {
			retval = getQuickRecharge(
				json.getNString("userID")
			);
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	
	public String fetchUserLimits(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		
		if (json.getErrorCode().equals("0")) {
			retval = fetchUserLimits(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String getOffers(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		
		if (json.getErrorCode().equals("0")) {
			retval = getOpOffers(
				json.getNString("operator"),json.getNString("flag"),json.getEString("lastUpdateTime")
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
	
	public String updatePendingTrx(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = updatePendingTrx(json.getNString("card_number"), json.getNString("user_id"), json.getNString("admin_id"), json.getNString("status"), json.getNString("guest_status"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String requestUpdateTrxUser(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = requestPendingTrxUser(json.getNString("trx_id"), json.getJsonObject().toString());
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
			retval = updatePayStatus(json.getNString("trx_id"), json.getEString("status"), json.getNString("additional_info"),json.getNString("trx_type"),
					json.getEString("LP_trx_status"),json.getNString("payment_method"),json.getEString("card_brand"),json.getNString("card_number"),
					json.getNString("bank"),json.getNString("bkash_payment_number"),json.getNString("billing_name"),json.getEString("card_region"),json.getEString("binIssuerCountry"),json.getEString("binIssuerBank"));
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
			retval = getStatus(json.getNString("user_id"),json.getNString("trx_id"),json.getNString("user_trx_id"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchUserRates(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchUserRates(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String getFileTopSummary(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = getFileTopupSummary(json.getNString("userID"),json.getNString("fileID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String getFileTopHistory(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = getFileTopupHistory(json.getNString("userID"));
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
	
	public String fetchStockHistory(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchStocksHistory(json.getNString("userID"), json.getEString("startDate"), json.getEString("endDate"));
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
	
	public String allocateShadowOpBalance(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = allocateShadowOpBalance(json.getNString("userID"), json.getNString("operator"), json.getNString("amount"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String checkUpdates(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = checkForUpdates(json.getNString("userID"), json.getNString("appVersion"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchOpBalance(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchOpBalance();
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchShadowOpBalance(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchShadowOpBalance();
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchAllBalance(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchAllBalance(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String getUploadStatus(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchUploadStatus(json.getNString("user_id"), json.getNString("fileID"), (json.isParameterPresent("extended")?json.getNString("extended"):""));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String getDownloadStatus(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchDownloadStatus(json.getNString("userID"), json.getNString("downloadID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchUserCardList(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchUserCardList(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchUserCardCounts(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchUserCardCounts(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchPendingTrxUser(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchPendingTrxUser(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchPendingTrxAdmin(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchPendingTrxAdmin(json.getNString("userID"));
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
	
	public String fetchShadowOpBalanceHistory(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchShadowOpBalanceHistory(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchFileTopUpHistory(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchFileTopUpHistory(json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchSingleFileTopUpHistory(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchSingleFileTopUps(json.getNString("userID"),json.getNString("fileID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchUploadedFileDetails(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchUploadedFileDetail(json.getNString("fileID"),json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String downloadFileTopupReport(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = downloadUploadedFileDetails(json.getNString("fileID"),json.getNString("userID"),json.getNString("outputType"),json.getNString("reportType"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String fetchInvalidFileRows(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = fetchInvalidFileRow(json.getNString("fileID"),json.getNString("userID"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
	
	public String processFileTopup(String message, String messageBody) {
		String retval = "E";
		JsonDecoder json;
		if (messageBody.isEmpty()) {
			json = new JsonDecoder(message);
		} else {
			json = new JsonDecoder(messageBody);
		}
		if (json.getErrorCode().equals("0")) {
			retval = processFileTopups(json.getNString("userID"), json.getNString("fileID"), json.getNString("processFlag"));
		} else {
			retval = "E:JSON string invalid";
		}
		return retval;
	}
}
