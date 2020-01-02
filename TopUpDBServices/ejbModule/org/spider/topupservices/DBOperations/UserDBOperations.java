/**
 * 
 */
package org.spider.topupservices.DBOperations;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Engine.LoginProcessor;
import org.spider.topupservices.Engine.RegistrationProcessor;
import org.spider.topupservices.Engine.SmsSender;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;
import org.spider.topupservices.Utilities.RandomStringGenerator;

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

	public JsonEncoder verifyAppUser(String appname, String password, String appToken) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String accessFlag = "-1";
		String errorMessage = "General Error";

		String type = "0";
		String validity = "0";
		
		if ((NullPointerExceptionHandler.isNullOrEmpty(appname) || NullPointerExceptionHandler.isNullOrEmpty(password)) && NullPointerExceptionHandler.isNullOrEmpty(appToken)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		}else {
			List<String> emptyList = new ArrayList<>();
			emptyList.add("");
			emptyList.add("0");
			emptyList.add("0");
			

			List<String> appInfo = this.configurations.getTopUpUsers().containsKey(appname)
					? this.configurations.getTopUpUsers().get(appname)
					: emptyList;

			type = appInfo.get(1);
			validity = appInfo.get(2);
			
			String appPass = appInfo.get(0);

			if(NullPointerExceptionHandler.isNullOrEmpty(appToken)) {
				if (password.equals(appPass) && appPass != "") {
					errorCode = "0";
					errorMessage = "Application authentication successful.";
					
					if(Configurations.checkValidUserToken.containsKey(appname)) {
						String oldToken=Configurations.checkValidUserToken.get(appname);
						Configurations.checkValidUserToken.remove(appname);
						
						if(Configurations.authenticationTokenHM.containsKey(oldToken)) 
						Configurations.authenticationTokenHM.remove(oldToken);
						
						//LogWriter.LOGGER.info("inside validity check after " + oldToken);
					}
					
					// add token
					//AuthenticationToken at= new AuthenticationToken();
					//String tokenId=at.generateNewTokenId(user_id);
					//TODO check if it always returns token or not 
					String tokenn= Configurations.generateNewTokenId(appname, Integer.parseInt(validity));
					Configurations.checkValidUserToken.put(appname, tokenn);
					
					if(type.equals("1")) {
						accessFlag = "0";
					}
					jsonEncoder.addElement("appToken", tokenn);
				} else {
					errorCode = "-5";
					errorMessage = "User is not authorized to perform this action.";
				}
			} else {
				// verify token
				String retval=new LoginProcessor(weTopUpDS, logWriter, configurations).checkApiToken(appToken);
				JsonDecoder jd = new JsonDecoder(retval);
				
				if(jd.getEString("ErrorCode").equals("0000")) {
					errorCode = "0";
					accessFlag = "0";
					errorMessage = "appToken is valid.";
				} else if(jd.getEString("ErrorCode").equals("0003")) {
					errorCode = "100";
					errorMessage = "No appToken found.";
				} else if(jd.getEString("ErrorCode").equals("0004")) {
					errorCode = "101";
					errorMessage = "appToken Expired.";
				}
				
			}
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("type", type);
		jsonEncoder.addElement("accessFlag", accessFlag);
		jsonEncoder.addElement("validity", validity);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public String insertTopUpTransaction(String user_id, String operator, String opType, String payee_name,
			String payee_phone, String payee_email, String amount, String trx_id, String topup_trx_id, String remarks,
			String test, String additional_info) {

		String errorCode = "-1";

		if (NullPointerExceptionHandler.isNullOrEmpty(user_id) || NullPointerExceptionHandler.isNullOrEmpty(amount)
				|| NullPointerExceptionHandler.isNullOrEmpty(trx_id)
				|| NullPointerExceptionHandler.isNullOrEmpty(operator)
				|| NullPointerExceptionHandler.isNullOrEmpty(opType)
				|| NullPointerExceptionHandler.isNullOrEmpty(payee_phone)
				|| NullPointerExceptionHandler.isNullOrEmpty(topup_trx_id)) {
			errorCode = "5";
		} else {
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
					weTopUpDS.getPreparedStatement().setString(5, msisdnNormalize(payee_phone));
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

	public JsonEncoder topupFileInsert(String user_id, String filename, String updated_filename) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String errorCode = "-1";
		String errorMessage = "not inserted.";
		String fileID = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(user_id) || NullPointerExceptionHandler.isNullOrEmpty(filename)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sqlInsert = "INSERT INTO topup_file_info(file_name,updated_file_name,user_id) VALUES (?,?,?)";
			try {
				// json: file_name,school_id
				weTopUpDS.prepareStatement(sqlInsert, true);
				weTopUpDS.getPreparedStatement().setString(1, filename);
				weTopUpDS.getPreparedStatement().setString(2, updated_filename);
				weTopUpDS.getPreparedStatement().setString(3, user_id);
				try {
					weTopUpDS.execute();
					fileID = getNewInsertID();
					errorCode = "0";
					errorMessage = "upload successful.";
					LogWriter.LOGGER.info("fileID : " + fileID);
				} catch (SQLIntegrityConstraintViolationException de) {
					errorCode = "-1";
					errorMessage = "duplicate filename.";
					LogWriter.LOGGER.severe("SQLIntegrityConstraintViolationException:" + de.getMessage());
				} catch (SQLException e) {
					errorCode = "-11";
					errorMessage = "SQLException";
					LogWriter.LOGGER.severe("SQLException" + e.getMessage());
				}
				if (weTopUpDS.getConnection() != null)
					weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				errorCode = "-2";
				errorMessage = "SQLException";
				LogWriter.LOGGER.severe(e.getMessage());
			} catch (Exception e) {
				errorCode = "-3";
				errorMessage = "General Exception";
				LogWriter.LOGGER.severe(e.getMessage());
				e.printStackTrace();
			} /*
				 * finally{ if(weTopUpDS.getConnection() != null){ try {
				 * weTopUpDS.getConnection().close(); } catch (SQLException e) { errorCode="-4";
				 * LogWriter.LOGGER.severe(e.getMessage()); } } }/
				 **/

		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("Filename", filename);
		jsonEncoder.addElement("fileID", fileID);
		jsonEncoder.buildJsonObject();
		// errorCode=jsonEncoder;

		return jsonEncoder;
	}
	
	
	public JsonEncoder addQuickRecharge(String userID,String phone,String amount, String operator, String opType, String remarks) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String errorCode = "-1";
		String errorMessage = "not inserted.";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(phone) 
				|| NullPointerExceptionHandler.isNullOrEmpty(amount) || NullPointerExceptionHandler.isNullOrEmpty(operator) 
				|| NullPointerExceptionHandler.isNullOrEmpty(opType) || NullPointerExceptionHandler.isNullOrEmpty(remarks)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sqlInsert = "insert into quick_recharges (user_id, phone, operator, opType, amount, remarks) values (?, ?, ?, ?, ?, ?)";
			try {
				// json: file_name,school_id
				weTopUpDS.prepareStatement(sqlInsert, true);
				weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
				weTopUpDS.getPreparedStatement().setString(2, phone);
				weTopUpDS.getPreparedStatement().setString(3, operator);
				weTopUpDS.getPreparedStatement().setString(4, opType);
				weTopUpDS.getPreparedStatement().setString(5, amount);
				weTopUpDS.getPreparedStatement().setString(6, remarks);
				try {
					weTopUpDS.execute();
					errorCode = "0";
					errorMessage = "quick recharge creation successful.";
				} catch (SQLIntegrityConstraintViolationException de) {
					errorCode = "-1";
					errorMessage = "duplicate quick recharge.";
					LogWriter.LOGGER.severe("SQLIntegrityConstraintViolationException:" + de.getMessage());
				} catch (SQLException e) {
					errorCode = "-11";
					errorMessage = "SQLException";
					LogWriter.LOGGER.severe("SQLException" + e.getMessage());
				}
				if (weTopUpDS.getConnection() != null)
					weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				errorCode = "-2";
				errorMessage = "SQLException";
				LogWriter.LOGGER.severe(e.getMessage());
			} catch (Exception e) {
				errorCode = "-3";
				errorMessage = "General Exception";
				LogWriter.LOGGER.severe(e.getMessage());
				e.printStackTrace();
			}
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	
	public JsonEncoder modifyQuickRecharge(String userID,String quickID, String phone,String amount, String operator, String opType, String remarks, String flag) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sqlUpdate = "update quick_recharges set phone = ?, operator = ?, opType = ?, amount = ?, remarks = ? where user_id=? and id =?";
		String sqlDisable = "update quick_recharges set status = 1 where user_id=? and id =?";
		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(quickID) || NullPointerExceptionHandler.isNullOrEmpty(flag)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			try {
				if(flag.equals("0")) {	//	flag = 0 for update
					weTopUpDS.prepareStatement(sqlUpdate,true);
					weTopUpDS.getPreparedStatement().setString(1, phone);
					weTopUpDS.getPreparedStatement().setString(2, operator);
					weTopUpDS.getPreparedStatement().setString(3, opType);
					weTopUpDS.getPreparedStatement().setString(4, amount);
					weTopUpDS.getPreparedStatement().setString(5, remarks);
					weTopUpDS.getPreparedStatement().setInt(6, Integer.parseInt(userID));
					weTopUpDS.getPreparedStatement().setInt(7, Integer.parseInt(quickID));
					
					long count = weTopUpDS.executeUpdate();
					weTopUpDS.closePreparedStatement();
					if(count>0) {
						errorCode = "0";
						errorMessage = "modification successful.";
					} else {
						errorCode = "10";
						errorMessage = "modification failed";
					}
				}else if(flag.equals("1")) {//	flag = 1 for delete
					weTopUpDS.prepareStatement(sqlDisable,true);
					weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
					weTopUpDS.getPreparedStatement().setInt(2, Integer.parseInt(quickID));
					
					long count = weTopUpDS.executeUpdate();
					weTopUpDS.closePreparedStatement();
					if(count>0) {
						errorCode = "0";
						errorMessage = "modification successful.";
					} else {
						errorCode = "10";
						errorMessage = "modification failed";
					}
				} else {
					errorCode = "6";
					errorMessage = "Invalid modification request.";
				}
				
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
	
	public JsonEncoder getQuickRecharge(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";
		String quickRecharges = "";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(userID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sql = "SELECT id, phone, operator, opType, amount, remarks, insert_time, update_time FROM quick_recharges where user_id=? and status = 0";
			
						
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
					quickRecharges += rs.getString(1) + ",";
					quickRecharges += rs.getString(2) + ",";
					quickRecharges += rs.getString(3) + ",";
					quickRecharges += rs.getString(4) + ",";
					quickRecharges += rs.getString(5) + ",";
					quickRecharges += rs.getString(6) + ",";
					quickRecharges += rs.getString(7) + ",";
					quickRecharges += rs.getString(8) + "|";
				}
				int lio = quickRecharges.lastIndexOf("|");
				if (lio > 0)
					quickRecharges = quickRecharges.substring(0, lio);

				errorCode = "0";
				errorMessage = "getQuickRecharge successful";

				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("quickRecharges", quickRecharges);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getOffers(String operator,String flag, String lastUpdateTime) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";
		String offers = "";
		String offerUpdateTime = "";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(operator) || NullPointerExceptionHandler.isNullOrEmpty(flag)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			offerUpdateTime = fetchLastOfferUpdateTime();
			
			if(offerUpdateTime.equals(lastUpdateTime) && offerUpdateTime!="") {
				errorCode = "30";
				errorMessage = "Offers up-to-date.";
			} else {
				String sql = "SELECT id, amount, operator, opType, internet, minutes, sms, validity, status, insert_time, update_time, callRate "
						+ "FROM packages_info where " + (flag.equals("1")?("operator="+operator+" and"):"") + " status = 0 order by insert_time desc";
				
							
				try {
					weTopUpDS.prepareStatement(sql);
					ResultSet rs = weTopUpDS.executeQuery();
					while (rs.next()) {
						
						offers += "\"" +rs.getString(1) + "\"" + ",";
						offers += "\"" +rs.getString(2) + "\"" + ",";
						offers += "\"" +rs.getString(3) + "\"" + ",";
						offers += "\"" +rs.getString(4) + "\"" + ",";
						offers += "\"" +rs.getString(5) + "\"" + ",";
						offers += "\"" +rs.getString(6) + "\"" + ",";
						offers += "\"" +rs.getString(7) + "\"" + ",";
						offers += "\"" +rs.getString(8) + "\"" + ",";
						offers += "\"" +rs.getString(9) + "\"" + ",";
						offers += "\"" +rs.getString(10) + "\"" + ",";
						offers += "\"" +rs.getString(11) + "\"" + ",";
						offers += "\"" +rs.getString(12) + "\"" + "|";
					}
					int lio = offers.lastIndexOf("|");
					if (lio > 0)
						offers = offers.substring(0, lio);

					errorCode = "0";
					errorMessage = "getOffers successful";

					rs.close();
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e) {
					e.printStackTrace();
					errorCode = "11";
					errorMessage = "SQL Exception.";
					LogWriter.LOGGER.severe(e.getMessage());
				}
			}
			
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("offers", offers);
		jsonEncoder.addElement("lastUpdateTime", offerUpdateTime);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	/**
	 * 
	 * @return String groupID
	 * @throws SQLException
	 */
	private String getNewInsertID() throws SQLException {
		String retval = "-1";
		ResultSet rs = weTopUpDS.getGeneratedKeys();
		if (rs.next()) {
			retval = rs.getString(1);
		}
		rs.close();
		return retval;
	}

	public JsonEncoder insertTransaction(String user_id, String amount, String trx_id, String user_trx_id, String src,
			String trx_type, String operator, String opType, String payee_name, String payee_phone, String payee_email,
			String remarks, String test, String ref_file_id, String overrideBalance) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String trxErrorCode = "-1";
		String trxErrorMessage = "General Error";
		String topUpErrorCode = "-1";
		String topUpErrorMessage = "General Error";
		String opBalanceFlag = "";
		String userBalanceFlag = "";
		String additional_info = null;
		double operatorBalance = 0.0;
		double userBalance = 0.0;
		String accessKey = "";
		String topup_trx_id = "";
		String payment_method = "";
		String api_retry_profile = "0";
		
		payee_phone = NullPointerExceptionHandler.isNullOrEmpty(payee_phone)?"":payee_phone;
		
		if (NullPointerExceptionHandler.isNullOrEmpty(user_id) || NullPointerExceptionHandler.isNullOrEmpty(amount)
				|| NullPointerExceptionHandler.isNullOrEmpty(trx_id)
				|| NullPointerExceptionHandler.isNullOrEmpty(trx_type)) {
			trxErrorCode = "5";
			trxErrorMessage = "Missing one or more parameters for insertTransactionLog.";
			
		} else if(new Login(weTopUpDS, configurations, logWriter).isUserBlocked(user_id)) {
			LogWriter.LOGGER.info("User is blocked.");
			trxErrorCode = "50"; // User is blocked.
			trxErrorMessage = "User is blocked.";
			jsonEncoder.addElement("trxErrorCode", trxErrorCode);
			jsonEncoder.addElement("ErrorCode", trxErrorCode);
			jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
			jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
			jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
			
			jsonEncoder.buildJsonObject();
			return jsonEncoder;
//		} else if (amount.matches("[0-9]+") && (amount.length() > 1 || !trx_type.equals("0"))) {
//		} else if (amount.matches("[0-9]+") && (amount.length() > 1)) {
		} else if (amount.matches("[0-9]+") && (Integer.parseInt(amount) > 9) && (Integer.parseInt(amount) <=1000 )) {	
			String stockConfigurations = fetchUserStockConfigurations(user_id);
			
			if (NullPointerExceptionHandler.isNullOrEmpty(operator)) {
				operator = "10"; // for balance
			}
			String userType = getUserType(user_id);
			
			if (trx_type.equals("0") || trx_type.equals("2") || trx_type.equals("4") || trx_type.equals("5")) {
				payment_method = "0";
				
				if(userType.equals("10")) {
					LogWriter.LOGGER.info("Topup Limit overrided for Trusted Retailer.");
				} else {
					JsonDecoder jdTrx = new JsonDecoder(getUserOperationConfig(user_id).getJsonObject().toString());
					int maxTopupCount = Integer.parseInt(jdTrx.getNString("maximumTopupCount"));
					double maxTopupAmount = Double.parseDouble(jdTrx.getNString("maximumTopupAmount"));
					int maxFailedCount = Integer.parseInt(jdTrx.getNString("maximumFailedCount"));
					double maxFailedAmount = Double.parseDouble(jdTrx.getNString("maximumFailedAmount"));
					
					if (jdTrx.getNString("ErrorCode").equals("0")) {
						
						// check if Topup limit exceeded for user
						jdTrx = new JsonDecoder(getUserTopupSummary(user_id).getJsonObject().toString());

						int topupCount = Integer.parseInt(jdTrx.getNString("topupCount"));
						double topupAmount = Double.parseDouble(jdTrx.getNString("topupAmount"));
						topupAmount = topupAmount + Double.parseDouble(amount);
						
						if(topupCount>=maxTopupCount || topupAmount>maxTopupAmount) {
							trxErrorCode = "27";
							trxErrorMessage = "Topup limit exceeded.";
							LogWriter.LOGGER.info(trxErrorMessage);

							jsonEncoder.addElement("ErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
							jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
							jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
							jsonEncoder.addElement("accessKey", accessKey);
							jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
							jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

							jsonEncoder.buildJsonObject();
							// errorCode=jsonEncoder;

							return jsonEncoder;
						}
						
						// check if Failed limit exceeded for user
						jdTrx = new JsonDecoder(getUserFailedSummary(user_id).getJsonObject().toString());

						int failedCount = Integer.parseInt(jdTrx.getNString("failedCount"));
						double failedAmount = Double.parseDouble(jdTrx.getNString("failedAmount"));
						
						if(failedCount>=maxFailedCount || failedAmount>maxFailedAmount) {
							trxErrorCode = "26";
							trxErrorMessage = "Failed limit exceeded.";
							
							LogWriter.LOGGER.info(trxErrorMessage);

							jsonEncoder.addElement("ErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
							jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
							jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
							jsonEncoder.addElement("accessKey", accessKey);
							jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
							jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

							jsonEncoder.buildJsonObject();
							// errorCode=jsonEncoder;

							return jsonEncoder;
						}
					} else {
						trxErrorCode = "23";
						trxErrorMessage = "Failed to load topup history.";
						LogWriter.LOGGER.info(trxErrorMessage);

						jsonEncoder.addElement("ErrorCode", trxErrorCode);
						jsonEncoder.addElement("trxErrorCode", trxErrorCode);
						jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
						jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
						jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
						jsonEncoder.addElement("accessKey", accessKey);
						jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
						jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

						jsonEncoder.buildJsonObject();
						// errorCode=jsonEncoder;

						return jsonEncoder;
					}
				}
			} else {
				payment_method = "1";
				// check if Stock Refill allowed for user
				// 0 = inactive; 1 = active;		bitwise	:		0 = visibility;	1 = history;	2 = topup;	3 = refill;	4 = transfer;
				if (stockConfigurations.charAt(3) == '0') {
					trxErrorCode = "32"; // Stock Refill not permitted
					trxErrorMessage = "Stock Refill not permitted";
					
					jsonEncoder.addElement("trxErrorCode", trxErrorCode);
					jsonEncoder.addElement("ErrorCode", trxErrorCode);
					jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
					jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
					jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
					
					jsonEncoder.buildJsonObject();
					// errorCode=jsonEncoder;

					return jsonEncoder;
				}
				
				if(userType.equals("10")) {
					LogWriter.LOGGER.info("Stock Refill Limit overrided for Trusted Retailer.");
				} else {
					JsonDecoder jdTrx = new JsonDecoder(getUserOperationConfig(user_id).getJsonObject().toString());
					double maxStockAmount = Double.parseDouble(jdTrx.getNString("maximumStockAmount"));
					int maxStockRefillCount = Integer.parseInt(jdTrx.getNString("maximumStockRefillCount"));
					double maxStockRefillAmount = Double.parseDouble(jdTrx.getNString("maximumStockRefillAmount"));
					int maxFailedCount = Integer.parseInt(jdTrx.getNString("maximumFailedCount"));
					double maxFailedAmount = Double.parseDouble(jdTrx.getNString("maximumFailedAmount"));
					
					userBalance = getUserBalance(user_id);
					LogWriter.LOGGER.info("userBalance : " + userBalance);
					
					if (jdTrx.getNString("ErrorCode").equals("0")) {
						
						// check if Stock Refill limit exceeded for user
						jdTrx = new JsonDecoder(getUserStockRefillSummary(user_id).getJsonObject().toString());

						int stockRefillCount = Integer.parseInt(jdTrx.getNString("stockRefillCount"));
						double stockRefillAmount = Double.parseDouble(jdTrx.getNString("stockRefillAmount"));
						stockRefillAmount = stockRefillAmount + Double.parseDouble(amount);
						
						if(stockRefillCount>=maxStockRefillCount || stockRefillAmount>maxStockRefillAmount || (Double.parseDouble(amount)+userBalance)>maxStockAmount) {
							trxErrorCode = "24";
							trxErrorMessage = "Stock Refill limit exceeded.";
							
							LogWriter.LOGGER.info(trxErrorMessage);

							jsonEncoder.addElement("ErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
							jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
							jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
							jsonEncoder.addElement("accessKey", accessKey);
							jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
							jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

							jsonEncoder.buildJsonObject();
							// errorCode=jsonEncoder;

							return jsonEncoder;
						}
						
						// check if Failed limit exceeded for user
						jdTrx = new JsonDecoder(getUserFailedSummary(user_id).getJsonObject().toString());

						int failedCount = Integer.parseInt(jdTrx.getNString("failedCount"));
						double failedAmount = Double.parseDouble(jdTrx.getNString("failedAmount"));
						
						if(failedCount>=maxFailedCount || failedAmount>maxFailedAmount) {
							trxErrorCode = "26";
							trxErrorMessage = "Failed limit exceeded.";
							
							LogWriter.LOGGER.info(trxErrorMessage);

							jsonEncoder.addElement("ErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
							jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
							jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
							jsonEncoder.addElement("accessKey", accessKey);
							jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
							jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

							jsonEncoder.buildJsonObject();
							// errorCode=jsonEncoder;

							return jsonEncoder;
						}
					} else {
						trxErrorCode = "23";
						trxErrorMessage = "Failed to load history.";
						LogWriter.LOGGER.info(trxErrorMessage);

						jsonEncoder.addElement("ErrorCode", trxErrorCode);
						jsonEncoder.addElement("trxErrorCode", trxErrorCode);
						jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
						jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
						jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
						jsonEncoder.addElement("accessKey", accessKey);
						jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
						jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

						jsonEncoder.buildJsonObject();
						// errorCode=jsonEncoder;

						return jsonEncoder;
					}
				}
				accessKey = fetchAccessKey(operator, test);

				//LogWriter.LOGGER.info("accessKey : " + accessKey);
			}
			//LogWriter.LOGGER.info("payment_method : " + payment_method);
			
			if(trx_type.equals("0")) {
				String opGrants = fetchUserApiOpGrants(user_id);

				//	check if Operator allowed for user
				if (opGrants.charAt(Integer.parseInt(operator)) == '1') {
					LogWriter.LOGGER.info("operator not permitted.");
					trxErrorCode = "30"; // OP not permitted
					trxErrorMessage = "operator not permitted";
					jsonEncoder.addElement("trxErrorCode", trxErrorCode);
					jsonEncoder.addElement("ErrorCode", trxErrorCode);
					jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
					jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
					jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
					
					jsonEncoder.buildJsonObject();
					return jsonEncoder;
				}
			}
			
			try {
				String sqlTransactionLog = "INSERT INTO transaction_log (ref_file_id,user_id,amount,trx_id,user_trx_id,source,payment_method,additional_info,trx_type,receiver_phone,retry_profile) "
						+ "select ?,?,?,?,?,?,?,?,?,?," + (src.equals("API") ? api_retry_profile
								: ("u.retry_profile from users_info u where u.user_id=" + user_id));

				try {
					weTopUpDS.prepareStatement(sqlTransactionLog, true);
					if (NullPointerExceptionHandler.isNullOrEmpty(ref_file_id)) {
						weTopUpDS.getPreparedStatement().setString(1,null);
					}else {
						weTopUpDS.getPreparedStatement().setString(1, ref_file_id);
					}
					
					
					weTopUpDS.getPreparedStatement().setString(2, user_id);
					weTopUpDS.getPreparedStatement().setString(3, amount);
					weTopUpDS.getPreparedStatement().setString(4, trx_id);
					weTopUpDS.getPreparedStatement().setString(5, user_trx_id);
					weTopUpDS.getPreparedStatement().setString(6, src);
					weTopUpDS.getPreparedStatement().setString(7, payment_method);
					weTopUpDS.getPreparedStatement().setString(8, additional_info);
					weTopUpDS.getPreparedStatement().setString(9, trx_type);
					weTopUpDS.getPreparedStatement().setString(10, msisdnNormalize(payee_phone));

					weTopUpDS.execute();

					trxErrorCode = "0";
					trxErrorMessage = "successfully inserted into transaction_log";

					LogWriter.LOGGER.info("trxErrorMessage : " + trxErrorMessage);

					// for Balance Deduct
					if (trx_type.equals("4")) {
						userBalance = getUserBalance(user_id);
						LogWriter.LOGGER.info("userBalance : " + userBalance);

						if (userBalance < Double.parseDouble(amount)) {
							userBalanceFlag = "5";
							topUpErrorCode = "5";
							topUpErrorMessage = "Insufficient user balance.";
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + topUpErrorMessage;

							updateTransactionStatus(trx_id, "1", additional_info, trx_type).getJsonObject().toString();
							additional_info = "";

						} else {
							userBalanceFlag = "0";

							boolean deductFlag = false;
							if (payment_method.equals("0") && userBalanceFlag.equals("0")) {

								deductFlag = deductUserBalance(user_id, Double.parseDouble(amount));
								LogWriter.LOGGER.info("balance deduction : " + deductFlag);

								additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
										: (additional_info + " | ")) + "Customer Previous Balance: " + userBalance;

								if (deductFlag) {
									userBalance = getUserBalance(user_id);
									additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
											: (additional_info + " | ")) + "Customer Updated Balance: "
											+ userBalance;
									JsonDecoder json = new JsonDecoder(
											updateTransactionStatus(trx_id, "2", additional_info, trx_type)
													.getJsonObject().toString());
									additional_info = "";
									topUpErrorCode = json.getNString("ErrorCode");
									topUpErrorMessage = json.getNString("ErrorMessage");

									if (topUpErrorCode.equals("0")) {
									} else {
										topUpErrorCode = "14";
										topUpErrorMessage = "TRX_status update failed for User balance deduction.";

										boolean flag = false;

										flag = rechargeUserBalanceByID(user_id, Double.parseDouble(amount));
										if (flag) {
											topUpErrorCode = "16";
											topUpErrorMessage = "update failed, balance refunded.";
											additional_info = (NullPointerExceptionHandler
													.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
													+ topUpErrorMessage + " | Distributor Previous Balance: "
													+ userBalance + " | Distributor Updated Balance: "
													+ getUserBalance(user_id);

											updateTransactionStatus(trx_id, "1", additional_info, trx_type);
											additional_info = "";
										} else {
											topUpErrorCode = "17";
											topUpErrorMessage = "balance refunded, update failed.";
											additional_info = (NullPointerExceptionHandler
													.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
													+ topUpErrorMessage;

											updateTransactionStatus(trx_id, "1", additional_info, trx_type);
											additional_info = "";
										}
										jsonEncoder.addElement("ErrorCode", trxErrorCode);
										jsonEncoder.addElement("trxErrorCode", trxErrorCode);
										jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
										jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
										jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
										jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);
										
										

										jsonEncoder.buildJsonObject();
										// errorCode=jsonEncoder;

										return jsonEncoder;
									}
								} else {
									topUpErrorCode = "15";
									topUpErrorMessage = "User balance deduction failed.";

									jsonEncoder.addElement("ErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
									jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
									jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
									jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

									jsonEncoder.buildJsonObject();
									// errorCode=jsonEncoder;

									return jsonEncoder;
								}
							}
						}
					}
					
					// for Admin Balance Transfer
					if (trx_type.equals("2")) {
						// check if Stock Transfer allowed for user
						// 0 = inactive; 1 = active;		bitwise	:		0 = visibility;	1 = history;	2 = topup;	3 = refill;	4 = transfer;
						if (stockConfigurations.charAt(4) == '0') {
							topUpErrorCode = "31"; // Stock Transfer not permitted
							topUpErrorMessage = "Stock Transfer not permitted";
							LogWriter.LOGGER.info(topUpErrorMessage);
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + topUpErrorMessage;

							updateTransactionStatus(trx_id, "1", additional_info, trx_type);
							additional_info = "";

							jsonEncoder.addElement("ErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
							jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
							jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
							jsonEncoder.addElement("accessKey", accessKey);
							jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
							jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

							jsonEncoder.buildJsonObject();
							// errorCode=jsonEncoder;

							return jsonEncoder;
						}
						boolean retailerFlag = false;
						retailerFlag = checkRetailer(payee_phone);
						if (retailerFlag) {
							LogWriter.LOGGER.info("Retailer found.");
						} else {
							topUpErrorCode = "13";
							topUpErrorMessage = "Retailer does not exist.";
							LogWriter.LOGGER.info(topUpErrorMessage);

							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + topUpErrorMessage;

							updateTransactionStatus(trx_id, "1", additional_info, trx_type);
							additional_info = "";

							jsonEncoder.addElement("ErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
							jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
							jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
							jsonEncoder.addElement("accessKey", accessKey);
							jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
							jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

							jsonEncoder.buildJsonObject();
							// errorCode=jsonEncoder;

							return jsonEncoder;
						}

						userBalance = getUserBalance(user_id);
						LogWriter.LOGGER.info("userBalance : " + userBalance);

						if (userBalance < Double.parseDouble(amount)) {
							userBalanceFlag = "5";
							topUpErrorCode = "5";
							topUpErrorMessage = "Insufficient user balance.";
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + topUpErrorMessage;

							updateTransactionStatus(trx_id, "1", additional_info, trx_type).getJsonObject().toString();
							additional_info = "";

						} else {
							userBalanceFlag = "0";

							boolean deductFlag = false;
							if (payment_method.equals("0") && userBalanceFlag.equals("0")) {

								deductFlag = deductUserBalance(user_id, Double.parseDouble(amount));
								LogWriter.LOGGER.info("balance deduction : " + deductFlag);

								additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
										: (additional_info + " | ")) + "Distributor Previous Balance: " + userBalance;

								if (deductFlag) {
									userBalance = getUserBalance(user_id);
									additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
											: (additional_info + " | ")) + "Distributor Updated Balance: "
											+ userBalance;
									JsonDecoder json = new JsonDecoder(
											updateTransactionStatus(trx_id, "2", additional_info, trx_type)
													.getJsonObject().toString());
									additional_info = "";
									topUpErrorCode = json.getNString("ErrorCode");
									topUpErrorMessage = json.getNString("ErrorMessage");

									if (topUpErrorCode.equals("0")) {
									} else {
										topUpErrorCode = "14";
										topUpErrorMessage = "TRX_status update failed for User balance deduction.";

										boolean flag = false;

										flag = rechargeUserBalanceByID(user_id, Double.parseDouble(amount));
										if (flag) {
											topUpErrorCode = "16";
											topUpErrorMessage = "update failed, balance refunded.";
											additional_info = (NullPointerExceptionHandler
													.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
													+ topUpErrorMessage + " | Distributor Previous Balance: "
													+ userBalance + " | Distributor Updated Balance: "
													+ getUserBalance(user_id);

											updateTransactionStatus(trx_id, "1", additional_info, trx_type);
											additional_info = "";
										} else {
											topUpErrorCode = "17";
											topUpErrorMessage = "balance refunded, update failed.";
											additional_info = (NullPointerExceptionHandler
													.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
													+ topUpErrorMessage;

											updateTransactionStatus(trx_id, "1", additional_info, trx_type);
											additional_info = "";
										}

										jsonEncoder.addElement("ErrorCode", trxErrorCode);
										jsonEncoder.addElement("trxErrorCode", trxErrorCode);
										jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
										jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
										jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
										jsonEncoder.addElement("accessKey", accessKey);
										jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
										jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

										jsonEncoder.buildJsonObject();
										// errorCode=jsonEncoder;

										return jsonEncoder;
									}
								} else {
									topUpErrorCode = "15";
									topUpErrorMessage = "User balance deduction failed.";

									jsonEncoder.addElement("ErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
									jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
									jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
									jsonEncoder.addElement("accessKey", accessKey);
									jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
									jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

									jsonEncoder.buildJsonObject();
									// errorCode=jsonEncoder;

									return jsonEncoder;
								}
							}
						}
					}
					
					// for User Stock Transfer
					if (trx_type.equals("5")) {
						// check if Stock Transfer allowed for user
						// 0 = inactive; 1 = active;		bitwise	:		0 = visibility;	1 = history;	2 = topup;	3 = refill;	4 = transfer;
						if (stockConfigurations.charAt(4) == '0') {
							// trxErrorMessage = "Stock Transfer not permitted";
							topUpErrorCode = "31"; // Stock Transfer not permitted
							topUpErrorMessage = "Stock Transfer not permitted";
							LogWriter.LOGGER.info(topUpErrorMessage);
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + topUpErrorMessage;

							updateTransactionStatus(trx_id, "1", additional_info, trx_type);
							additional_info = "";

							jsonEncoder.addElement("ErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
							jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
							jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
							jsonEncoder.addElement("accessKey", accessKey);
							jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
							jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

							jsonEncoder.buildJsonObject();
							// errorCode=jsonEncoder;

							return jsonEncoder;
						}
						
						if(userType.equals("10")) {
							LogWriter.LOGGER.info("Transfer Limit overrided for Trusted Retailer.");
						} else {
							JsonDecoder jdTrx = new JsonDecoder(getUserOperationConfig(user_id).getJsonObject().toString());
							int maxTransferCount = Integer.parseInt(jdTrx.getNString("maximumTransferCount"));
							double maxTransferAmount = Double.parseDouble(jdTrx.getNString("maximumTransferAmount"));
							int maxFailedCount = Integer.parseInt(jdTrx.getNString("maximumFailedCount"));
							double maxFailedAmount = Double.parseDouble(jdTrx.getNString("maximumFailedAmount"));
							
							if (jdTrx.getNString("ErrorCode").equals("0")) {
								jdTrx = new JsonDecoder(getUserTransferSummary(user_id).getJsonObject().toString());

								int transferCount = Integer.parseInt(jdTrx.getNString("transferCount"));
								double transferAmount = Double.parseDouble(jdTrx.getNString("transferAmount"));
								transferAmount = transferAmount + Double.parseDouble(amount);
								
								if(transferCount>=maxTransferCount || transferAmount>maxTransferAmount) {
									topUpErrorCode = "22";
									topUpErrorMessage = "Transfer limit exceeded.";
									LogWriter.LOGGER.info(topUpErrorMessage);

									additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
											: (additional_info + " | ")) + topUpErrorMessage;

									updateTransactionStatus(trx_id, "1", additional_info, trx_type);
									additional_info = "";

									jsonEncoder.addElement("ErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
									jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
									jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
									jsonEncoder.addElement("accessKey", accessKey);
									jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
									jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

									jsonEncoder.buildJsonObject();
									// errorCode=jsonEncoder;

									return jsonEncoder;
								}
								
								// check if Failed limit exceeded for user
								jdTrx = new JsonDecoder(getUserFailedSummary(user_id).getJsonObject().toString());

								int failedCount = Integer.parseInt(jdTrx.getNString("failedCount"));
								double failedAmount = Double.parseDouble(jdTrx.getNString("failedAmount"));
								
								if(failedCount>=maxFailedCount || failedAmount>maxFailedAmount) {
									topUpErrorCode = "26";
									topUpErrorMessage = "Failed limit exceeded.";
									
									LogWriter.LOGGER.info(topUpErrorMessage);

									jsonEncoder.addElement("ErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
									jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
									jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
									jsonEncoder.addElement("accessKey", accessKey);
									jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
									jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

									jsonEncoder.buildJsonObject();
									// errorCode=jsonEncoder;

									return jsonEncoder;
								}
							} else {
								topUpErrorCode = "23";
								topUpErrorMessage = "Failed to load transfer history.";
								LogWriter.LOGGER.info(topUpErrorMessage);

								additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
										: (additional_info + " | ")) + topUpErrorMessage;

								updateTransactionStatus(trx_id, "1", additional_info, trx_type);
								additional_info = "";

								jsonEncoder.addElement("ErrorCode", trxErrorCode);
								jsonEncoder.addElement("trxErrorCode", trxErrorCode);
								jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
								jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
								jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
								jsonEncoder.addElement("accessKey", accessKey);
								jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
								jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

								jsonEncoder.buildJsonObject();
								// errorCode=jsonEncoder;

								return jsonEncoder;
							}
						}

						JsonDecoder checkJson = new JsonDecoder(new Login(this.weTopUpDS,this.configurations,this.logWriter).checkUserInDB(payee_phone).getJsonObject().toString());
						
						if (checkJson.getEString("ErrorCode").equals("0") && !checkJson.getEString("userFlag").equals("0")) {
							LogWriter.LOGGER.info("user found.");
						} else {
							topUpErrorCode = "13";
							topUpErrorMessage = "user does not exist.";
							LogWriter.LOGGER.info(topUpErrorMessage);

							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + topUpErrorMessage;

							updateTransactionStatus(trx_id, "1", additional_info, trx_type);
							additional_info = "";

							jsonEncoder.addElement("ErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorCode", trxErrorCode);
							jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
							jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
							jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
							jsonEncoder.addElement("accessKey", accessKey);
							jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
							jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

							jsonEncoder.buildJsonObject();
							// errorCode=jsonEncoder;

							return jsonEncoder;
						}
						
						userBalance = getUserBalance(user_id);
						LogWriter.LOGGER.info("userBalance : " + userBalance);

						if (userBalance < Double.parseDouble(amount)) {
							userBalanceFlag = "5";
							topUpErrorCode = "5";
							topUpErrorMessage = "Insufficient user balance.";
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + topUpErrorMessage;

							updateTransactionStatus(trx_id, "1", additional_info, trx_type).getJsonObject().toString();
							additional_info = "";

						} else {
							userBalanceFlag = "0";

							boolean deductFlag = false;
							if (payment_method.equals("0") && userBalanceFlag.equals("0")) {

								deductFlag = deductUserBalance(user_id, Double.parseDouble(amount));
								LogWriter.LOGGER.info("balance deduction : " + deductFlag);

								additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
										: (additional_info + " | ")) + "Distributor Previous Balance: " + userBalance;

								if (deductFlag) {
									userBalance = getUserBalance(user_id);
									additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
											: (additional_info + " | ")) + "Distributor Updated Balance: "
											+ userBalance;
									JsonDecoder json = new JsonDecoder(
											updateTransactionStatus(trx_id, "2", additional_info, trx_type)
													.getJsonObject().toString());
									additional_info = "";
									topUpErrorCode = json.getNString("ErrorCode");
									topUpErrorMessage = json.getNString("ErrorMessage");

									if (topUpErrorCode.equals("0")) {
									} else {
										topUpErrorCode = "14";
										topUpErrorMessage = "TRX_status update failed for User balance deduction.";

										boolean flag = false;

										flag = rechargeUserBalanceByID(user_id, Double.parseDouble(amount));
										if (flag) {
											topUpErrorCode = "16";
											topUpErrorMessage = "update failed, balance refunded.";
											additional_info = (NullPointerExceptionHandler
													.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
													+ topUpErrorMessage + " | Distributor Previous Balance: "
													+ userBalance + " | Distributor Updated Balance: "
													+ getUserBalance(user_id);

											updateTransactionStatus(trx_id, "1", additional_info, trx_type);
											additional_info = "";
										} else {
											topUpErrorCode = "17";
											topUpErrorMessage = "balance refunded, update failed.";
											additional_info = (NullPointerExceptionHandler
													.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
													+ topUpErrorMessage;

											updateTransactionStatus(trx_id, "1", additional_info, trx_type);
											additional_info = "";
										}

										jsonEncoder.addElement("ErrorCode", trxErrorCode);
										jsonEncoder.addElement("trxErrorCode", trxErrorCode);
										jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
										jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
										jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
										jsonEncoder.addElement("accessKey", accessKey);
										jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
										jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

										jsonEncoder.buildJsonObject();
										// errorCode=jsonEncoder;

										return jsonEncoder;
									}
								} else {
									topUpErrorCode = "15";
									topUpErrorMessage = "User balance deduction failed.";

									jsonEncoder.addElement("ErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorCode", trxErrorCode);
									jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
									jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
									jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
									jsonEncoder.addElement("accessKey", accessKey);
									jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
									jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

									jsonEncoder.buildJsonObject();
									// errorCode=jsonEncoder;

									return jsonEncoder;
								}
							}
						}
					}

					// for TOP UP
					if (trx_type.equals("0")) {
						userBalance = getUserBalance(user_id);
						LogWriter.LOGGER.info("userBalance : " + userBalance);

						String opConf = "";
						
//						0=Airtel ; 1=Robi ; 2=GrameenPhone ; 3=Banglalink ; 4=Teletalk
						LogWriter.LOGGER.info("operator : " + operator);
						
						opConf = getOpConfig(operator);
						
						LogWriter.LOGGER.info("opConf : " + opConf);
						
						operatorBalance = getShadowBalance(opConf);

						LogWriter.LOGGER.info("shadowOperatorBalance : " + operatorBalance);
						
						if (operatorBalance >= Integer.parseInt(amount)) {
							opBalanceFlag = "0";
						} else {
							opBalanceFlag = "5";
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + "Low operator balance.";
							LogWriter.LOGGER.info(additional_info);
						}

						// check if Recharge from Stock allowed for user
						// 0 = inactive; 1 = active;		bitwise	:		0 = visibility;	1 = history;	2 = topup;	3 = refill;	4 = transfer;
						if (stockConfigurations.charAt(2) == '0') {
//						    trxErrorCode = "31"; // Recharge from Stock not permitted
//						    trxErrorMessage = "Recharge from Stock not permitted";
						    LogWriter.LOGGER.info("Recharge from Stock not permitted");
						    overrideBalance = "1";
						}
						
						if(overrideBalance.equals("1")) {
							userBalanceFlag = "10";
							
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + "Overided topup from balance.";

							if (opBalanceFlag.equals("0")) {
								accessKey = fetchAccessKey(operator, test);

								LogWriter.LOGGER.info("accessKey : " + accessKey);
							}
						} else if (userBalance < Double.parseDouble(amount)) {
							userBalanceFlag = "5";
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + "Insufficient user balance.";

							if (opBalanceFlag.equals("0")) {
								accessKey = fetchAccessKey(operator, test);

								LogWriter.LOGGER.info("accessKey : " + accessKey);
							}
						} else {
							userBalanceFlag = "0";

							if (opBalanceFlag.equals("0")) {
								boolean deductUserFlag = false;
								
								if (payment_method.equals("0") && userBalanceFlag.equals("0")) {
									deductUserFlag = deductUserBalance(user_id, Double.parseDouble(amount));
									LogWriter.LOGGER.info("user balance deduction : " + deductUserFlag);

									additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
											: (additional_info + " | ")) + "Customer Previous Balance: " + userBalance;

									if (deductUserFlag) {
										userBalance = getUserBalance(user_id);
										additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info)
												? ""
												: (additional_info + " | "))
												+ "Balance deducted. | Customer Updated Balance: " + userBalance;

										JsonDecoder json = new JsonDecoder(
												updateTransactionStatus(trx_id, "2", additional_info, trx_type)
														.getJsonObject().toString());
										additional_info = "";
										topUpErrorCode = json.getNString("ErrorCode");
										topUpErrorMessage = json.getNString("ErrorMessage");

										if (topUpErrorCode.equals("0")) {

										} else {
											topUpErrorCode = "14";
											topUpErrorMessage = "TRX_status update failed for User balance deduction.";

											boolean flag = false;

											flag = rechargeUserBalanceByID(user_id, Double.parseDouble(amount));
											if (flag) {
												topUpErrorCode = "16";
												topUpErrorMessage = "update failed, balance refunded.";
												additional_info = (NullPointerExceptionHandler.isNullOrEmpty(
														additional_info) ? "" : (additional_info + " | "))
														+ topUpErrorMessage + " | Customer Previous Balance: "
														+ userBalance + " | Customer Updated Balance: "
														+ getUserBalance(user_id);

												updateTransactionStatus(trx_id, "1", additional_info, trx_type);
												additional_info = "";
											} else {
												topUpErrorCode = "17";
												topUpErrorMessage = "balance refunded, update failed.";
												additional_info = (NullPointerExceptionHandler.isNullOrEmpty(
														additional_info) ? "" : (additional_info + " | "))
														+ topUpErrorMessage;

												updateTransactionStatus(trx_id, "1", additional_info, trx_type);
												additional_info = "";
											}

											jsonEncoder.addElement("ErrorCode", trxErrorCode);
											jsonEncoder.addElement("trxErrorCode", trxErrorCode);
											jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
											jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
											jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
											jsonEncoder.addElement("accessKey", accessKey);
											jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
											jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

											jsonEncoder.buildJsonObject();
											// errorCode=jsonEncoder;

											return jsonEncoder;
										}
									} else {
										topUpErrorCode = "15";
										topUpErrorMessage = "User balance deduction failed.";
																				
										jsonEncoder.addElement("ErrorCode", trxErrorCode);
										jsonEncoder.addElement("trxErrorCode", trxErrorCode);
										jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
										jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
										jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
										jsonEncoder.addElement("accessKey", accessKey);
										jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
										jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);

										jsonEncoder.buildJsonObject();
										// errorCode=jsonEncoder;

										return jsonEncoder;
									}
								}
							}
						}

						topup_trx_id = RandomStringGenerator.getRandomString("0123456789", 2);
						// topup_trx_id = topup_trx_id +
						// RandomStringGenerator.getRandomString("abcdefghijklmnopqrstuvwxyz", 4);
						topup_trx_id = topup_trx_id + RandomStringGenerator
								.getRandomString("0123456789abcdefghABCDEFGHIJKLMNOPQRSTUVWXYZijklmnopqrstuvwxyz", 11);
						topup_trx_id = topup_trx_id
								+ RandomStringGenerator.getRandomString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 2) + "00";

						topUpErrorCode = insertTopUpTransaction(user_id, operator, opType, payee_name, payee_phone,
								payee_email, amount, trx_id, topup_trx_id, remarks, test, additional_info);
						if (topUpErrorCode.equals("0")) {
							topUpErrorMessage = "successfully inserted into topup_log";
							LogWriter.LOGGER.info(topUpErrorMessage);
						} else if (topUpErrorCode.equals("5")) {
							topUpErrorMessage = "Missing one or more parameters for insertTopUpTransaction";
							LogWriter.LOGGER.info(topUpErrorMessage);
						} else {
							topUpErrorMessage = "failed to insert into topup_log";

							LogWriter.LOGGER.info(topUpErrorMessage);
						}
					}

				} catch (SQLIntegrityConstraintViolationException de) {
					trxErrorCode = "1";// : Same name Already exists
					trxErrorMessage = "SQLIntegrityConstraintViolationExceptions";
					LogWriter.LOGGER.severe("SQLIntegrityConstraintViolationException:" + de.getMessage());
				} catch (SQLException e) {
					trxErrorCode = "11";// :Inserting parameters failed
					trxErrorMessage = "SQLException";
					e.printStackTrace();
					LogWriter.LOGGER.severe("SQLException" + e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
					trxErrorCode = "10"; // :other Exception
					trxErrorMessage = "other Exception";
					e.printStackTrace();
				}
			} finally {
				if (weTopUpDS.getConnection() != null) {
					try {
						weTopUpDS.closePreparedStatement();
						// weTopUpDS.getConnection().close();
					} catch (SQLException e) {
						trxErrorCode = "-4"; // :connection close Exception
						trxErrorMessage = "connection close Exception";
						e.printStackTrace();
						LogWriter.LOGGER.severe(e.getMessage());
					}
				}
			}
			// LogWriter.LOGGER.info("UserID:"+userId);

		}
		else {
			trxErrorCode = "-9";
			trxErrorMessage = "Invalid amount.";
		}

		jsonEncoder.addElement("ErrorCode", trxErrorCode);
		jsonEncoder.addElement("trxErrorCode", trxErrorCode);
		jsonEncoder.addElement("trxErrorMessage", trxErrorMessage);
		jsonEncoder.addElement("topUpErrorCode", topUpErrorCode);
		jsonEncoder.addElement("topUpErrorMessage", topUpErrorMessage);
		jsonEncoder.addElement("accessKey", accessKey);
		jsonEncoder.addElement("opBalanceFlag", opBalanceFlag);
		jsonEncoder.addElement("userBalanceFlag", userBalanceFlag);
		jsonEncoder.addElement("trx_id", trx_id);

		jsonEncoder.buildJsonObject();
		// errorCode=jsonEncoder;

		return jsonEncoder;
	}

	private String msisdnNormalize(String phoneNumber) {
		try {
			phoneNumber = phoneNumber.replace("+", "").replaceAll("-", "").replaceAll(" ", "");
			// phoneNumber = phoneNumber.replace("-", "");
			// phoneNumber = phoneNumber.replace(" ", "");

			if (phoneNumber.matches("^((880)|(0))?(1[3-9]{1}|35|44|66){1}[0-9]{8}$")) {
				// correct number of digits

				if (phoneNumber.startsWith("0"))
					phoneNumber = "88" + phoneNumber;
				else if (phoneNumber.startsWith("880")) {

				} else {
					phoneNumber = "880" + phoneNumber;
				}
			} else {
				// invalid
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return phoneNumber;
	}

	private String fetchLastOfferUpdateTime() {
		String lastUpdateTime = "";
		String sql = "SELECT update_time FROM packages_info order by update_time desc limit 1";
		try {
			weTopUpDS.prepareStatement(sql);
			
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				lastUpdateTime = rs.getString("update_time");
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		} catch (Exception e) {
			if (weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
			}
			LogWriter.LOGGER.severe(e.getMessage());
		}
		return lastUpdateTime;
	}
	
	private boolean checkRetailer(String msisdn) {
		boolean flag = false;
		String sql = "SELECT count(*) FROM users_info where user_type in (?,?) and phone=?";
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, "5"); // for Retailer
			weTopUpDS.getPreparedStatement().setString(2, "10"); // for Trusted Retailer
			msisdn = msisdnNormalize(msisdn);
			weTopUpDS.getPreparedStatement().setString(3, msisdn);

			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				if(rs.getInt(1)>0) {
					flag = true;
				} else {
					
				}
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		} catch (Exception e) {
			if (weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
			}
			LogWriter.LOGGER.severe(e.getMessage());
		}
		return flag;
	}
	
	public String sendSms(String action, String key, String phone, String trx_id) throws UnsupportedEncodingException {
		String smsbody = "WeTopUp Reset Password";
		
		smsbody = fetchSmsBody(action);
		
		//LogWriter.LOGGER.info("mailbody : " + mailbody);

		if (action.equals("resetPassword")) {
			smsbody = smsbody.replace("replace_key_here", key);
		} else if (action.equals("sendOTP")) {
			smsbody = smsbody.replace("replace_otp_here", key);
		} else if (action.equals("registrationSuccess")) {
			smsbody = smsbody.replace("replace_phone_here", phone);
		} else {
		}

		return new SmsSender(this.weTopUpDS, this.configurations, this.logWriter).sendSms(phone, smsbody).getJsonObject().toString();
		
	}

	public JsonEncoder sendEmail(String action, String key, String email, String trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String errorCode = "-1";
		String errorMessage = "General Error";

		String app_name = "we_top_up";
		String subject = "WeTopUp Recharge Service";
		String mailbody = "WeTopUp Reset Password";
		String amount = "";
		Double credit_amount = 0.0;
		Double commission_rate = 0.0;
		Double commission_amount = 0.0;
		String operator = "";
		String opType = "";
		String payee_phone = "";
		String time = "";
		String remarks = "";
		String payment_method = "";
		String payment_tool = "";
		String ref_trx_id = "";
		
		String userID = ""; 
		String source = "";
		String card_brand = "";
		String card_number = "";
		String bank = "";
		String billing_name = "";
		String trx_type = "";
		String bin_issuer_bank = "";
		String bin_issuer_country = "";

		subject = fetchMailSubject(action);
		mailbody = fetchMailBody(action);
		
		//LogWriter.LOGGER.info("mailbody : " + mailbody);

		if (action.equals("resetPassword")) {
			mailbody = mailbody.replace("replace_key_here", key);
		} else if (action.equals("sendOTP")) {
			mailbody = mailbody.replace("replace_otp_here", key);
		} else if (action.equals("registrationSuccess")) {
			mailbody = mailbody.replace("replace_phone_here", key);
			mailbody = mailbody.replace("replace_email_here", email);
		} else if (action.equals("topupSuccess") || action.equals("topupFailed")) {

			userID = getUserFromTrx(trx_id);

			JsonDecoder json = new JsonDecoder(getTopUpStatus(userID, trx_id, null).getJsonObject().toString());

			remarks = json.getNString("remarks");
			amount = json.getNString("amount");
			payee_phone = json.getNString("payee_phone");
			operator = getOperatorName(json.getNString("operator"));
			opType = getOpType(json.getNString("opType"));
			time = json.getNString("response_time");
			payment_method = json.getNString("payment_method");

			payment_method = getPaymentMethod(payment_method);

			mailbody = mailbody.replace("replace_remarks_here", remarks);
			mailbody = mailbody.replace("replace_operator_here", operator);
			mailbody = mailbody.replace("replace_phone_here", payee_phone);
			mailbody = mailbody.replace("replace_amount_here", amount);
			mailbody = mailbody.replace("replace_opType_here", opType);
			mailbody = mailbody.replace("replace_time_here", time);
			mailbody = mailbody.replace("replace_trx_id_here", trx_id);
			mailbody = mailbody.replace("replace_payment_method_here", payment_method);
		} else if (action.equals("notifyVoid")) {
			JsonDecoder json = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());

			amount = json.getNString("amount");
			time = json.getNString("update_time");
			userID = json.getNString("user_id"); 
			String userEmail = getUserEmail(userID);
			String userName = getUserName(userID);
			payee_phone = getUserPhone(userID);
			source = json.getNString("source");
			bin_issuer_bank = json.getNString("bin_issuer_bank");
			bin_issuer_country = json.getNString("bin_issuer_country");
			
			card_brand = json.getNString("card_brand");
			card_number = json.getNString("card_number");
			bank = json.getNString("bank");
			billing_name = json.getNString("billing_name");
			trx_type = json.getNString("trx_type");
			
			mailbody = mailbody.replace("replace_amount_here", amount);
			mailbody = mailbody.replace("replace_phone_here", payee_phone);
			mailbody = mailbody.replace("replace_time_here", time);
			mailbody = mailbody.replace("replace_trx_id_here", trx_id);
			mailbody = mailbody.replace("replace_user_id_here", userID);
			mailbody = mailbody.replace("replace_source_here", source);
			
			mailbody = mailbody.replace("replace_bin_issuer_bank_here", bin_issuer_bank);
			mailbody = mailbody.replace("replace_bin_issuer_country_here", bin_issuer_country);
			mailbody = mailbody.replace("replace_card_brand_here", card_brand);
			mailbody = mailbody.replace("replace_card_number_here", card_number);
			mailbody = mailbody.replace("replace_bank_here", bank);
			mailbody = mailbody.replace("replace_billing_name_here", billing_name);
			mailbody = mailbody.replace("replace_email_here", userEmail);
			mailbody = mailbody.replace("replace_name_here", userName);
			mailbody = mailbody.replace("replace_trx_type_here", (trx_type.contains("0")?"topup":"stock refill"));
			
		} else {
			JsonDecoder json = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());

			amount = json.getNString("amount");
			credit_amount = Double.parseDouble(json.getNString("credit_amount"));
			commission_amount = Double.parseDouble(json.getNString("commission_amount"));
			commission_rate = Double.parseDouble(json.getNString("commission_rate"));
			payee_phone = json.getNString("receiver_phone");
			time = json.getNString("update_time");
			payment_method = json.getNString("payment_method");
			
			payment_tool = json.getEString("card_brand");
			
			if(payment_tool.equals("")) {
				payment_tool = json.getEString("bank");
			}
			ref_trx_id = json.getNString("ref_trx_id");

			payment_method = getPaymentMethod(payment_method);

			mailbody = mailbody.replace("replace_remarks_here", remarks);
			mailbody = mailbody.replace("replace_operator_here", operator);
			mailbody = mailbody.replace("replace_phone_here", payee_phone);
			mailbody = mailbody.replace("replace_amount_here", amount);
			mailbody = mailbody.replace("replace_credit_amount_here", "" + credit_amount);
			mailbody = mailbody.replace("replace_commission_rate_here", ""+commission_rate);
			mailbody = mailbody.replace("replace_commission_amount_here", ""+commission_amount);
			mailbody = mailbody.replace("replace_opType_here", opType);
			mailbody = mailbody.replace("replace_time_here", time);
			mailbody = mailbody.replace("replace_trx_id_here", trx_id);
			mailbody = mailbody.replace("replace_ref_trx_id_here", ref_trx_id);
			mailbody = mailbody.replace("replace_payment_method_here", payment_method);
			mailbody = mailbody.replace("replace_payment_tool_here", "("+payment_tool+").");
		}

		String to_address = email;
		String from_address = "WeTopUp <support@we-top-up.com>";
		String cc_address = "";
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
		// jsonEncoder.addElement("accessKey", accessKey);

		jsonEncoder.buildJsonObject();
		// errorCode=jsonEncoder;

		return jsonEncoder;
	}

	public String getOperatorName(String opCode) {
		// 0=Airtel ; 1=Robi ; 2=GrameenPhone ; 3=Banglalink ; 4=Teletalk
		if (opCode.equals("0")) {
			return "Airtel";
		} else if (opCode.equals("1")) {
			return "Robi";
		} else if (opCode.equals("2")) {
			return "GrameenPhone";
		} else if (opCode.equals("3")) {
			return "Banglalink";
		} else if (opCode.equals("4")) {
			return "Teletalk";
		} else {
			return "";
		}
	}

	public String getOpType(String opCode) {
		// 1=Prepaid ; 2=Postpaid
		if (opCode.equals("1")) {
			return "Prepaid";
		} else if (opCode.equals("2")) {
			return "Postpaid";
		} else {
			return "";
		}
	}

	public String[] fetchTempConfig(String action) throws Exception {
		String[] retval;
		String jsonReqName = "";
		String jsonReqPath = "";
		String templateID = "";

		try {
			String sql = "select c.templateID, t.req_file_name, t.req_file_location from template_configuration c left outer join template_table t on c.templateID = t.ID where c.action=?";

			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, action);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				jsonReqName = rs.getString("req_file_name");
				jsonReqPath = rs.getString("req_file_location");
				templateID = rs.getString("templateID");
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		retval = new String[] { jsonReqName, jsonReqPath, templateID };
		return retval;
	}

	public String getFileString(String filename, String path) throws IOException {
		File fl = new File(path + "/" + filename);

		String targetFileStr = new String(Files.readAllBytes(Paths.get(fl.getAbsolutePath())));
		return targetFileStr;
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

	public JsonEncoder updatePendingTrx(String card_number, String user_id, String admin_id, String status,
			String guest_status) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		String trx_id = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(user_id) || NullPointerExceptionHandler.isNullOrEmpty(card_number)
				|| NullPointerExceptionHandler.isNullOrEmpty(admin_id)
				|| NullPointerExceptionHandler.isNullOrEmpty(status)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters for insertTransactionLog.";
		} else if (checkIfAdmin(admin_id)) {
			String sql = "select trx_id from cards_list where user_id=? and card_number=? and status=?";

			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, user_id);
				weTopUpDS.getPreparedStatement().setString(2, card_number);
				weTopUpDS.getPreparedStatement().setInt(3, 0); // pending status
				ResultSet rs = weTopUpDS.executeQuery();
				if (status.equals("1")) {
					while (rs.next()) {
						trx_id = rs.getString(1);
						JsonDecoder jd = new JsonDecoder(insertTopupRefund(trx_id, user_id).getJsonObject().toString());
						if (jd.getNString("trxErrorCode").equals("0")
								&& jd.getNString("balanceErrorCode").equals("0")) {
							// success update
							updatePendingTrxStatus(1, guest_status, trx_id, admin_id);
							updateTransactionStatus(trx_id, "8", "refunded", "").getJsonObject().toString();
						} else {
							// fail update
							updatePendingTrxStatus(5, guest_status, trx_id, admin_id);
						}
					}
				} else if (status.equals("2")) {
					while (rs.next()) {
						trx_id = rs.getString(1);
						// fail update
						updatePendingTrxStatus(2, guest_status, trx_id, admin_id);
						updateTransactionStatus(trx_id, "7", "void requested", "").getJsonObject().toString();
					}
				}
				weTopUpDS.closePreparedStatement();
				rs.close();
				errorCode = "0";
				errorMessage = "Update successful.";
			} catch (SQLException e) {
				e.printStackTrace();
				LogWriter.LOGGER.severe(e.getMessage());
				errorCode = "11";
				errorMessage = "SQL Exception";
			}
		} else {
			errorCode = "25";
			errorMessage = "User is not authorized to perform this action.";
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder requestPendingTrxUser(String trx_id, String jsonReq) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "update cards_list set status=?, json_req = ? where trx_id = ? and status not in (0)";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, 0);
			weTopUpDS.getPreparedStatement().setString(2, jsonReq);
			weTopUpDS.getPreparedStatement().setString(3, trx_id);
			long count = weTopUpDS.executeUpdate();
			weTopUpDS.closePreparedStatement();
			if(count>0) {
				errorCode = "0";
				errorMessage = "Update successful.";
				sendEmail("trxRequestPending", null, "support@we-top-up.com", trx_id);
			} else {
				errorCode = "10";
				errorMessage = "update failed";
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

	public JsonEncoder updatePendingTrxStatus(int status, String guest_status, String trx_id, String admin_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "update cards_list set status=?,guest_status=?, updated_by=? where trx_id = ?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, status);
			weTopUpDS.getPreparedStatement().setInt(2, Integer.parseInt(guest_status));
			weTopUpDS.getPreparedStatement().setString(3, admin_id);
			weTopUpDS.getPreparedStatement().setString(4, trx_id);
			long count = weTopUpDS.executeUpdate();
			weTopUpDS.closePreparedStatement();
			if(count>0) {
				errorCode = "0";
				errorMessage = "Update successful.";
			} else {
				errorCode = "10";
				errorMessage = "update failed";
			}
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

	public JsonEncoder updateCardListUser(String user_id, String trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "update cards_list set user_id=? where trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, user_id);
			weTopUpDS.getPreparedStatement().setString(2, trx_id);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			errorCode = "0";
			errorMessage = "Update successful.";
			LogWriter.LOGGER.info(errorMessage);
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			errorCode = "11";
			errorMessage = "SQL Exception";
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		LogWriter.LOGGER.info("updateCardList retval : " + jsonEncoder.getJsonObject().toString());
		return jsonEncoder;
	}

	public boolean checkIfAlreadyUpdated(String trx_id, String status) {
		boolean flag = false;
		JsonDecoder jd = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());
		String trx_status = jd.getNString("trx_status");

		if ((Integer.parseInt(status) >= Integer.parseInt(trx_status)) && (Integer.parseInt(trx_status)!=2)) {
			flag = true;
		}
		return flag;
	}

	public JsonEncoder insertTopupRefund(String trx_id, String user_id) {
		JsonEncoder jsonEncoderInput = new JsonEncoder();
		JsonEncoder jsonEncoderOutput = new JsonEncoder();
		String trxErrorCode = "-1";
		String trxErrorMessage = "Not initiated";
		String balanceErrorCode = "-1";
		String balanceErrorMessage = "Not initiated";
		Double balance = 0.0;
		String additional_info = "";
		String amount = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(trx_id)) {
			trxErrorCode = "5";
			trxErrorMessage = "Missing one or more parameters.";
		} else {
			try {
				if (user_id.equals("0")) {
					insertRefundFails(trx_id);
				} else {
					String sqlTransactionLog = "INSERT INTO transaction_log (user_id,amount,trx_id,ref_trx_id,payment_method,trx_type,trx_status,receiver_phone) select user_id,amount,?,?,?,?,?,receiver_phone from transaction_log where trx_id=?";

					String newTrx_id = RandomStringGenerator
							.getRandomString("0123456789abcdefghABCDEFGHIJKLMNOPQRSTUVWXYZijklmnopqrstuvwxyz", 10);

					weTopUpDS.prepareStatement(sqlTransactionLog, true);
					weTopUpDS.getPreparedStatement().setString(1, newTrx_id);
					weTopUpDS.getPreparedStatement().setString(2, trx_id);
					weTopUpDS.getPreparedStatement().setString(3, "0");
					weTopUpDS.getPreparedStatement().setString(4, "3");
					weTopUpDS.getPreparedStatement().setString(5, "0");
					weTopUpDS.getPreparedStatement().setString(6, trx_id);

					weTopUpDS.execute();

					trxErrorCode = "0";
					trxErrorMessage = "successfully inserted into transaction_log";

					LogWriter.LOGGER.info("trxErrorMessage : " + trxErrorMessage);

					balance = getUserBalance(user_id);
					additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
							: (additional_info + " | ")) + "Customer Previous Balance: " + balance;
					// for Balance Refund
					boolean balFlag = false;
					JsonDecoder jd = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());
					amount = jd.getNString("amount");
					LogWriter.LOGGER.info("amount : " + amount);
					String email = getUserEmail(user_id);
					LogWriter.LOGGER.info("email : " + email);

					balFlag = rechargeUserBalanceByID(user_id, Double.parseDouble(amount));

					if (balFlag) {
						balance = getUserBalance(user_id);
						additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
								: (additional_info + " | ")) + "Customer Updated Balance: " + balance;
						balanceErrorCode = "0";
						balanceErrorMessage = "Balance refund successful.";
						updateBalTRXStatus(newTrx_id, "2", "4", additional_info);

						if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

						} else {
							try {
								sendEmail("balanceRefundSuccess", null, email, newTrx_id);
								updateEmailStatus(newTrx_id, "2");
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					} else {
						balanceErrorCode = "5";
						balanceErrorMessage = "Balance refund failed.";
						updateBalTRXStatus(newTrx_id, "1", "10", additional_info);
						insertRefundFails(trx_id);
					}
				}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				trxErrorCode = "11";
				trxErrorMessage = "SQLException";
			} finally {
				if (weTopUpDS.getConnection() != null) {
					try {
						weTopUpDS.closePreparedStatement();
						// weTopUpDS.getConnection().close();
					} catch (SQLException e) {
						trxErrorCode = "-4"; // :connection close Exception
						e.printStackTrace();
						trxErrorMessage = "Connection close Exception";
						LogWriter.LOGGER.severe(e.getMessage());
					}
				}
			}
		}
		jsonEncoderInput.addElement("trx_id", trx_id);
		jsonEncoderInput.addElement("user_id", user_id);
		jsonEncoderInput.addElement("amount", amount);

		jsonEncoderInput.buildJsonObject();

		jsonEncoderOutput.addElement("ErrorCode", trxErrorCode);
		jsonEncoderOutput.addElement("trxErrorCode", trxErrorCode);
		jsonEncoderOutput.addElement("trxErrorMessage", trxErrorMessage);
		jsonEncoderOutput.addElement("balanceErrorCode", balanceErrorCode);
		jsonEncoderOutput.addElement("balanceErrorMessage", balanceErrorMessage);

		jsonEncoderOutput.buildJsonObject();
		return jsonEncoderOutput;
	}

	private boolean updateBalTRXStatus(String trxID, String trxStatus, String balStatus, String additional_info) {
		boolean retval = false;
		String sqlUpdateUser = "UPDATE transaction_log set update_time=now(), trx_status=?, bal_rec_status=?,additional_info = case when additional_info is null and ? is not null then ? "
				+ "when additional_info is not null and ? is not null then concat(additional_info,' | ',?) else additional_info end  where trx_id=?";
		try {
			weTopUpDS.prepareStatement(sqlUpdateUser);
			weTopUpDS.getPreparedStatement().setString(1, trxStatus);
			weTopUpDS.getPreparedStatement().setString(2, balStatus);
			weTopUpDS.getPreparedStatement().setString(3, additional_info);
			weTopUpDS.getPreparedStatement().setString(4, additional_info);
			weTopUpDS.getPreparedStatement().setString(5, additional_info);
			weTopUpDS.getPreparedStatement().setString(6, additional_info);
			weTopUpDS.getPreparedStatement().setString(7, trxID);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			LogWriter.LOGGER.info("updateBalTRXStatus(): bal_rec_status for trxID : " + balStatus);
			retval = true;
		} catch (SQLException e) {
			LogWriter.LOGGER.severe("updateBalTRXStatus(): " + e.getMessage());
		}
		return retval;
	}

	public String insertRefundFails(String trx_id) {
		JsonEncoder jsonEncoderInput = new JsonEncoder();
		JsonEncoder jsonEncoderOutput = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Not initiated.";

		if (NullPointerExceptionHandler.isNullOrEmpty(trx_id)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			try {
				String sqlTopUpRetry = "insert into refund_fails (trx_id) VALUES (?)";

				try {
					weTopUpDS.prepareStatement(sqlTopUpRetry, true);
					weTopUpDS.getPreparedStatement().setString(1, trx_id);
					weTopUpDS.execute();

					errorCode = "0";
					errorMessage = "Successfully inserted into refund_fails.";

				} catch (SQLIntegrityConstraintViolationException de) {
					errorCode = "1";// : Same name Already exists
					errorMessage = "SQLIntegrityConstraintViolationException";
					LogWriter.LOGGER.severe("SQLIntegrityConstraintViolationException:" + de.getMessage());
				} catch (SQLException e) {
					errorCode = "11";// :Inserting parameters failed
					errorMessage = "SQLException";
					e.printStackTrace();
					LogWriter.LOGGER.severe("SQLException" + e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
					errorCode = "10"; // :other Exception
					errorMessage = "General Exception";
					e.printStackTrace();
				}
			} finally {
				if (weTopUpDS.getConnection() != null) {
					try {
						weTopUpDS.closePreparedStatement();
						// weTopUpDS.getConnection().close();
					} catch (SQLException e) {
						errorCode = "-4"; // :connection close Exception
						errorMessage = "Connection close Exception";
						e.printStackTrace();
						LogWriter.LOGGER.severe(e.getMessage());
					}
				}
			}
		}

		jsonEncoderInput.addElement("trx_id", trx_id);

		jsonEncoderInput.buildJsonObject();
		jsonEncoderOutput.addElement("ErrorCode", errorCode);
		jsonEncoderOutput.addElement("ErrorMessage", errorMessage);

		jsonEncoderOutput.buildJsonObject();
		return errorCode;
	}

	public JsonEncoder updateTransactionStatus(String trx_id, String status, String additional_info, String trx_type) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		additional_info = "";
		String balErrorCode = "-1";
		String balErrorMessage = "Not initiated.";
		
		LogWriter.LOGGER.info("UPDATE : " + trx_id + "	" + status + "	" + additional_info);

		if (checkIfAlreadyUpdated(trx_id, status)) {
			String sql = "UPDATE `transaction_log` SET update_time=now(), trx_status = ?, additional_info = case when additional_info is null and ? is not null then ? "
					+ "when additional_info is not null and ? is not null then concat(additional_info,' | ',?) else additional_info end WHERE trx_id=? and trx_status in (0,1,2,5)";

			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, status);
				weTopUpDS.getPreparedStatement().setString(2, additional_info);
				weTopUpDS.getPreparedStatement().setString(3, additional_info);
				weTopUpDS.getPreparedStatement().setString(4, additional_info);
				weTopUpDS.getPreparedStatement().setString(5, additional_info);
				weTopUpDS.getPreparedStatement().setString(6, trx_id);
				

				long count = weTopUpDS.executeUpdate();
				weTopUpDS.closePreparedStatement();
				if(count>0) {
					errorCode = "0";
					errorMessage = "modification successful.";
					
					try {

//						for balance recharge
						if (status.equals("2") && trx_type.equals("1")) {
							// recharge balance
							JsonDecoder jd = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());
							String user_id = jd.getNString("user_id");
							String balFlag = jd.getNString("bal_rec_status");
							
							Double amount = Double.parseDouble(jd.getNString("amount"));
							Double userBalance = 0.0;

							boolean flag = false;
							LogWriter.LOGGER.info("user_id : " + user_id);
							if (balFlag.equals("0")) {
								flag = rechargeUserBalanceByID(user_id, amount);
								if (flag) {
									balErrorCode = "0";
									balErrorMessage = "Successfully updated balance";
									additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
											: (additional_info + " | ")) + "Customer Previous Balance: " + userBalance
											+ " | Customer Updated Balance: " + getUserBalance(user_id);

									updateBalTrxStatus(trx_id, "4", additional_info, trx_type);

									String phone = getUserPhone(user_id);
									String email = getUserEmail(user_id);

									try {
//										send email
										if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

										} else {
											new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balancePurchaseSuccess", phone, email, trx_id);
											updateEmailStatus(trx_id, "2");
										}
									} catch (Exception e) {
										e.printStackTrace();
									}
								} else {
									balErrorCode = "5";
									balErrorMessage = "Failed to update balance";
									updateBalTrxStatus(trx_id, "10", additional_info, trx_type);
									String phone = getUserPhone(user_id);
									String email = getUserEmail(user_id);

									try {
//										send email
										if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

										} else {
											new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balancePurchaseFailed", phone, email, trx_id);
											updateEmailStatus(trx_id, "2");
										}
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							}

						}

						// for Admin balance transfer
						if (status.equals("2") && trx_type.equals("2")) {
							// recharge balance
							JsonDecoder jd = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());
							String payee_phone = jd.getNString("receiver_phone");
							Double amount = Double.parseDouble(jd.getNString("amount"));
							Double creditAmount = 0.0;
							Double commissionAmount = 0.0;
							Double commissionRate = 0.0;
							String balFlag = jd.getNString("bal_rec_status");
							Double userBalance = 0.0;

							jd = new JsonDecoder(getUserOperationConfig(payee_phone).getJsonObject().toString());

							if (jd.getNString("ErrorCode").equals("0")) {
								commissionRate = Double.parseDouble(jd.getNString("cashRate"));	// cash for balance transfer
								
								commissionAmount = calculateCommissionAmount(amount, commissionRate);
								
								LogWriter.LOGGER.info("Commission amount : "+commissionAmount);
								
								if(jd.getEString("floor").equals("1")) {
									commissionAmount = Math.ceil(commissionAmount);
									LogWriter.LOGGER.info("Commission amount (ceil): "+commissionAmount);
								} else if(jd.getEString("floor").equals("2")) {
									LogWriter.LOGGER.info("Commission amount unchanged");
								} else {
									commissionAmount = Math.floor(commissionAmount);
									LogWriter.LOGGER.info("Commission amount (floor) : "+commissionAmount);
								}

								creditAmount = commissionAmount + amount;

								userBalance = getUserBalance(jd.getNString("user_id"));

								boolean flag = false;
								LogWriter.LOGGER.info("receiver_phone : " + payee_phone);
								if (balFlag.equals("0")) {

									flag = rechargeUserBalanceByID(jd.getNString("user_id"), creditAmount);
									LogWriter.LOGGER.info("balance transfered : " + flag);

									if (flag) {
										balErrorCode = "0";

										additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
												: (additional_info + " | ")) + "Retailer Previous Balance: " + userBalance
												+ " | Retailer Updated Balance: "
												+ getUserBalance(jd.getNString("user_id"));

										balErrorMessage = "Successfully updated balance";
										updateBalTrxStatus(trx_id, "4", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

										String phone = getUserPhone(jd.getNString("user_id"));
										String email = getUserEmail(jd.getNString("user_id"));

										try {
//											send email
											if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

											} else {
												if(commissionAmount==0.0) {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balanceTransferSuccess", phone, email, trx_id);
												} else {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balanceTransferWithCommissionSuccess", phone, email, trx_id);
												}
												updateEmailStatus(trx_id, "2");
											}
										} catch (Exception e) {
											e.printStackTrace();
										}

									} else {
										balErrorCode = "5";
										balErrorMessage = "Failed to update balance";
										updateBalTrxStatus(trx_id, "10", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

//										//	send email
									}
								}
							} else {
								balErrorCode = "-4";
								balErrorMessage = "Failed to fetch commission rate";
							}
						}
						
						// for user balance transfer
						if (status.equals("2") && trx_type.equals("5")) {
							// recharge balance
							JsonDecoder jd = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());
							String payee_phone = jd.getNString("receiver_phone");
							String senderID = jd.getNString("user_id");
							Double amount = Double.parseDouble(jd.getNString("amount"));
							Double creditAmount = 0.0;
							Double commissionAmount = 0.0;
							Double commissionRate = 0.0;
							String balFlag = jd.getNString("bal_rec_status");
							Double userBalance = 0.0;

							jd = new JsonDecoder(getUserOperationConfig(senderID).getJsonObject().toString());

							if (jd.getNString("ErrorCode").equals("0")) {
								
								commissionRate = Double.parseDouble(jd.getNString("transferRate"));
								
								commissionAmount = calculateCommissionAmount(amount, commissionRate);
								
								LogWriter.LOGGER.info("charge amount : "+commissionAmount);
								
								if(jd.getEString("floor").equals("1")) {
									commissionAmount = Math.floor(commissionAmount);	//	reverse for deduction
									LogWriter.LOGGER.info("Charge amount (floor): "+commissionAmount);
								} else if(jd.getEString("floor").equals("2")) {
									LogWriter.LOGGER.info("Charge amount unchanged");
								} else {
									commissionAmount = Math.ceil(commissionAmount);	//	reverse for deduction
									LogWriter.LOGGER.info("Charge amount (ceil) : "+commissionAmount);
								}
								
								Double maxCharge = Double.parseDouble(jd.getNString("maximumChargeAmount"));
								Double minCharge = Double.parseDouble(jd.getNString("minimumChargeAmount"));
								
								if(maxCharge==0) {
									maxCharge = commissionAmount;
								}
								
								if(commissionAmount > maxCharge) {
									commissionAmount = maxCharge; 
									LogWriter.LOGGER.info("charge overided to : " + maxCharge);
								} else if(commissionAmount < minCharge) {
									commissionAmount = minCharge; 
									LogWriter.LOGGER.info("charge overided to : " + minCharge);
								} else {
									LogWriter.LOGGER.info("charge within range.");
								}

								creditAmount = amount - commissionAmount;

								userBalance = getUserBalance(getUserID(payee_phone));

								boolean flag = false;
								LogWriter.LOGGER.info("receiver : " + payee_phone);
								if (balFlag.equals("0")) {

									flag = rechargeUserBalanceByID(getUserID(payee_phone), creditAmount);
									LogWriter.LOGGER.info("balance transfered : " + flag);

									if (flag) {
										balErrorCode = "0";

										additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
												: (additional_info + " | ")) + "Customer Previous Balance: " + userBalance
												+ " | Customer Updated Balance: "
												+ getUserBalance(getUserID(payee_phone));

										balErrorMessage = "Successfully updated balance";
										updateBalTrxStatus(trx_id, "4", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

										String phone = getUserPhone(getUserID(payee_phone));
										String email = getUserEmail(getUserID(payee_phone));

										try {
//											send email
											if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

											} else {
												if(commissionAmount==0.0) {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balanceTransferSuccess", phone, email, trx_id);
												} else {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balanceTransferWithCommissionSuccess", phone, email, trx_id);
												}
												updateEmailStatus(trx_id, "2");
											}
										} catch (Exception e) {
											e.printStackTrace();
										}

									} else {
										balErrorCode = "5";
										balErrorMessage = "Failed to update balance";
										updateBalTrxStatus(trx_id, "10", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

//										//	send email
									}
								}
							} else {
								balErrorCode = "-4";
								balErrorMessage = "Failed to fetch commission rate";
							}
						}
						
						LogWriter.LOGGER.info("balErrorMessage : " + balErrorMessage);
						jsonEncoder.addElement("balErrorCode", balErrorCode);
						jsonEncoder.addElement("balErrorMessage", balErrorMessage);
					} catch (Exception e) {
						LogWriter.LOGGER.severe(e.getMessage());
						balErrorCode = "-3";
						balErrorMessage = "Failed to update balance";
						jsonEncoder.addElement("balErrorCode", balErrorCode);
						jsonEncoder.addElement("balErrorMessage", balErrorMessage);
					}
				} else {
					errorCode = "10";
					errorMessage = "modification failed";
				}
				
			} catch (SQLException e) {
				LogWriter.LOGGER.severe(e.getMessage());
				errorCode = "11";
				errorMessage = "SQL Exception";
			}
		} else {
			errorCode = "10";
			errorMessage = "Already updated.";
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder updateTransactionStatus(String trx_id, String status, String additional_info, String trx_type,
			String LP_trx_status, String payment_method, String card_brand, String card_number, String bank,
			String bkash_payment_number, String billing_name, String card_region, String binIssuerCountry, String binIssuerBank) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		boolean blockFlag = false;

		String lpFlag = NullPointerExceptionHandler.isNullOrEmpty(LP_trx_status)?"":LP_trx_status;
		
		LogWriter.LOGGER.info("UPDATE : trx_id : " + trx_id 
				+ "\nstatus 				: " + status 
				+ "\nadditional_info		:	" + additional_info 
				+ "\nLP_trx_status			:	" + LP_trx_status
				+ "\npayment_method			:	" + payment_method 
				+ "\ncard_brand				:	" + card_brand 
				+ "\ncard_number			:	" + card_number 
				+ "\nbank					:	" + bank 
				+ "\nbkash_payment_number	:	" + bkash_payment_number 
				+ "\nbilling_name			:	" + billing_name 
				+ "\nbilling_name			:	" + card_region
				+ "\nbinIssuerCountry		:	" + binIssuerCountry
				+ "\nbinIssuerBank			:	" + binIssuerBank);

		if (checkIfAlreadyUpdated(trx_id, status)) {
			JsonDecoder jd = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());
			String user_id = jd.getNString("user_id");

			if (NullPointerExceptionHandler.isNullOrEmpty(card_number)) {

			} else if (status.equals("2") && card_region.equals("0") && !binIssuerCountry.equalsIgnoreCase("bangladesh")) {
				blockFlag = checkIfCardIsBlocked(user_id, card_number, trx_id);
				if (blockFlag) {
					status = "5"; // card is blocked
					LogWriter.LOGGER.info("CARD IS BLOCKED");
				}
			}
			
			if (status.equals("2") && !lpFlag.equalsIgnoreCase("Success") ) {
				status = "1";
				LogWriter.LOGGER.info("LEBUPAY PAYMENT FAILED : " + status +"	lpFlag : "+lpFlag);
			}

			String sql = "UPDATE `transaction_log` SET update_time=now(), trx_status = ?, LP_trx_status = ?, payment_method = case when ? is not null then ? else payment_method end, additional_info = case when additional_info is null and ? is not null then ? "
					+ "when additional_info is not null and ? is not null then concat(additional_info,' | ',?) else additional_info end, card_brand = ?, card_number = ?, card_region = ?, bin_issuer_country = ?, bin_issuer_bank = ?, bank = ?, bkash_payment_number = ?, billing_name = ? WHERE trx_id=? and trx_status in (0,1)";

			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, status);
				weTopUpDS.getPreparedStatement().setString(2, LP_trx_status);
				weTopUpDS.getPreparedStatement().setString(3, payment_method);
				weTopUpDS.getPreparedStatement().setString(4, payment_method);
				weTopUpDS.getPreparedStatement().setString(5, additional_info);
				weTopUpDS.getPreparedStatement().setString(6, additional_info);
				weTopUpDS.getPreparedStatement().setString(7, additional_info);
				weTopUpDS.getPreparedStatement().setString(8, additional_info);
				weTopUpDS.getPreparedStatement().setString(9, card_brand);
				weTopUpDS.getPreparedStatement().setString(10, card_number);
				weTopUpDS.getPreparedStatement().setString(11, card_region);
				weTopUpDS.getPreparedStatement().setString(12, binIssuerCountry);
				weTopUpDS.getPreparedStatement().setString(13, binIssuerBank);
				weTopUpDS.getPreparedStatement().setString(14, bank);
				weTopUpDS.getPreparedStatement().setString(15, bkash_payment_number);
				weTopUpDS.getPreparedStatement().setString(16, billing_name);
				weTopUpDS.getPreparedStatement().setString(17, trx_id);
				weTopUpDS.execute();
				weTopUpDS.closePreparedStatement();
				if (blockFlag) {
					errorCode = "2";
					errorMessage = "Update successful, Card blocked.";

					//notify admin
					sendEmail("notifyVoid", "", "support@we-top-up.com", trx_id);
//					sendEmail("notifyVoid", "", "shaker@spiderdxb.com", trx_id);
					
					
				} else {
					errorCode = "0";
					errorMessage = "Update successful.";
				}

				additional_info = "";
				String balErrorCode = "-1";
				String balErrorMessage = "Not initiated.";
				try {
					//	for balance recharge, trx_type = 1
					//	for balance transfer, trx_type = 2
					
					if (status.equals("2") && (trx_type.equals("1") || trx_type.equals("2") || trx_type.equals("5"))) {
						// recharge balance
						String id = "";
						String receiverPhone = "";
						Double amount = Double.parseDouble(jd.getNString("amount"));
						Double creditAmount = 0.0;
						Double commissionAmount = 0.0;
						Double commissionRate = 0.0;
						String balFlag = jd.getNString("bal_rec_status");
						Double userBalance = 0.0;

						//	for balance recharge, trx_type = 1
						if(trx_type.equals("1")) {
							id = jd.getNString("user_id");
							
							jd = new JsonDecoder(getUserOperationConfig(id).getJsonObject().toString());

							if (jd.getNString("ErrorCode").equals("0")) {
								//	decide bank rate
								
								if(amount>=Double.parseDouble(jd.getNString("minimumRechargeAmount"))) {
									String tempBank = NullPointerExceptionHandler.isNullOrEmpty(bank)?"":bank;
									String tempCard = NullPointerExceptionHandler.isNullOrEmpty(card_brand)?"":card_brand;
									
									if(tempCard.equalsIgnoreCase("VISA")) {
										commissionRate = Double.parseDouble(jd.getNString("visaRate"));
									} else if(tempCard.equalsIgnoreCase("MASTERCARD") || tempCard.equalsIgnoreCase("MC")) {
										commissionRate = Double.parseDouble(jd.getNString("masterRate"));
									} else if(tempCard.equalsIgnoreCase("AMEX")) {
										commissionRate = Double.parseDouble(jd.getNString("amexRate"));
									} else if(tempBank.equalsIgnoreCase("BKASH")) {
										commissionRate = Double.parseDouble(jd.getNString("bkashRate"));
									}
									
									LogWriter.LOGGER.info("Commission applied : "+commissionRate+"%");
								} else {
									LogWriter.LOGGER.info("No Commission applied.");
									commissionRate = 0.0;
								}
								
							
								commissionAmount = calculateCommissionAmount(amount, commissionRate);

								LogWriter.LOGGER.info("Commission amount : "+commissionAmount);
								
								if(jd.getEString("floor").equals("1")) {
									commissionAmount = Math.ceil(commissionAmount);
									LogWriter.LOGGER.info("Commission amount (ceil): "+commissionAmount);
								} else if(jd.getEString("floor").equals("2")) {
									LogWriter.LOGGER.info("Commission amount unchanged");
								} else {
									commissionAmount = Math.floor(commissionAmount);
									LogWriter.LOGGER.info("Commission amount (floor) : "+commissionAmount);
								}
								
								
								creditAmount = commissionAmount + amount;

								userBalance = getUserBalance(jd.getNString("user_id"));

								boolean flag = false;
								
								LogWriter.LOGGER.info("receiverID : " + id);
								
								if (balFlag.equals("0")) {

									flag = rechargeUserBalanceByID(id, creditAmount);
									LogWriter.LOGGER.info("balance recharged : " + flag);

									if (flag) {
										balErrorCode = "0";

										additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
												: (additional_info + " | ")) + "Customer Previous Balance: " + userBalance
												+ " | Customer Updated Balance: "
												+ getUserBalance(jd.getNString("user_id"));

										balErrorMessage = "Successfully updated balance";
										updateBalTrxStatus(trx_id, "4", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

										String phone = getUserPhone(jd.getNString("user_id"));
										String email = getUserEmail(jd.getNString("user_id"));

										try {
//											send email
											if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

											} else {
												if(commissionAmount==0.0) {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balancePurchaseSuccess", phone, email, trx_id);
												} else {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balancePurchaseWithCommissionSuccess", phone, email, trx_id);
												}
												
												updateEmailStatus(trx_id, "2");
											}
										} catch (Exception e) {
											e.printStackTrace();
										}
									} else {
										balErrorCode = "5";
										balErrorMessage = "Failed to update balance";
										updateBalTrxStatus(trx_id, "10", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

										String phone = getUserPhone(user_id);
										String email = getUserEmail(user_id);

										try {
//											send email
											if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

											} else {
												new UserDBOperations(weTopUpDS, configurations, logWriter).sendEmail("balancePurchaseFailed", phone, email, trx_id);
												updateEmailStatus(trx_id, "2");
											}
										} catch (Exception e) {
											e.printStackTrace();
										}
									}
								}
							} else {
								balErrorCode = "-4";
								balErrorMessage = "Failed to fetch commission rate";
							}
						} 
						
						//	for admin balance transfer, trx_type = 2
						else if(trx_type.equals("2")) {
							id = jd.getNString("receiver_phone");
							
							jd = new JsonDecoder(getUserOperationConfig(id).getJsonObject().toString());

							if (jd.getNString("ErrorCode").equals("0")) {
								commissionRate = Double.parseDouble(jd.getNString("cashRate"));

								commissionAmount = calculateCommissionAmount(amount, commissionRate);
								
								LogWriter.LOGGER.info("Commission amount : "+commissionAmount);
								
								if(jd.getEString("floor").equals("1")) {
									commissionAmount = Math.ceil(commissionAmount);
									LogWriter.LOGGER.info("Commission amount (ceil): "+commissionAmount);
								} else if(jd.getEString("floor").equals("2")) {
									LogWriter.LOGGER.info("Commission amount unchanged");
								} else {
									commissionAmount = Math.floor(commissionAmount);
									LogWriter.LOGGER.info("Commission amount (floor) : "+commissionAmount);
								}

								creditAmount = commissionAmount + amount;

								userBalance = getUserBalance(jd.getNString("user_id"));

								boolean flag = false;
								
								LogWriter.LOGGER.info("receiverPhone : " + id);
								
								if (balFlag.equals("0")) {

									flag = rechargeUserBalanceByID(jd.getNString("user_id"), creditAmount);
									LogWriter.LOGGER.info("balance Transfered : " + flag);

									if (flag) {
										balErrorCode = "0";

										additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
												: (additional_info + " | ")) + "Customer Previous Balance: " + userBalance
												+ " | Customer Updated Balance: "
												+ getUserBalance(jd.getNString("user_id"));

										balErrorMessage = "Successfully updated balance";
										updateBalTrxStatus(trx_id, "4", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

										String phone = getUserPhone(jd.getNString("user_id"));
										String email = getUserEmail(jd.getNString("user_id"));

										try {
//											send email
											if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

											} else {
												
												if(commissionAmount==0.0) {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balanceTransferSuccess", phone, email, trx_id);
												} else {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balanceTransferWithCommissionSuccess", phone, email, trx_id);
												}
												
												updateEmailStatus(trx_id, "2");
											}
										} catch (Exception e) {
											e.printStackTrace();
										}
									} else {
										balErrorCode = "5";
										balErrorMessage = "Failed to update balance";
										updateBalTrxStatus(trx_id, "10", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

//											send email
									}
								}
							} else {
								balErrorCode = "-4";
								balErrorMessage = "Failed to fetch commission rate";
							}
						}
//						for user balance transfer, trx_type = 5
						else if(trx_type.equals("2")) {
							id = jd.getNString("user_id");
							receiverPhone = jd.getNString("receiver_phone");
							
							jd = new JsonDecoder(getUserOperationConfig(id).getJsonObject().toString());

							if (jd.getNString("ErrorCode").equals("0")) {
								commissionRate = Double.parseDouble(jd.getNString("transferRate"));

								commissionAmount = calculateCommissionAmount(amount, commissionRate);
								
								LogWriter.LOGGER.info("Commission amount : "+commissionAmount);
								
								if(jd.getEString("floor").equals("1")) {	//	reverse for deduct
									commissionAmount = Math.floor(commissionAmount);
									LogWriter.LOGGER.info("Commission amount (floor): "+commissionAmount);
								} else if(jd.getEString("floor").equals("2")) {
									LogWriter.LOGGER.info("Commission amount unchanged");
								} else {
									commissionAmount = Math.ceil(commissionAmount);	//	reverse for deduct
									LogWriter.LOGGER.info("Commission amount (ceil) : "+commissionAmount);
								}

								Double maxCharge = Double.parseDouble(jd.getNString("maximumChargeAmount"));
								Double minCharge = Double.parseDouble(jd.getNString("minimumChargeAmount"));
								
								if(commissionAmount > maxCharge) {
									commissionAmount = maxCharge; 
									LogWriter.LOGGER.info("charge overided to : " + maxCharge);
								} else if(commissionAmount < minCharge) {
									commissionAmount = minCharge; 
									LogWriter.LOGGER.info("charge overided to : " + minCharge);
								} else {
									LogWriter.LOGGER.info("charge within range.");
								}
								
								creditAmount = commissionAmount + amount;

								userBalance = getUserBalance(jd.getNString("user_id"));

								boolean flag = false;
								
								LogWriter.LOGGER.info("receiverPhone : " + id);
								
								if (balFlag.equals("0")) {

									flag = rechargeUserBalanceByID(jd.getNString("user_id"), creditAmount);
									LogWriter.LOGGER.info("balance Transfered : " + flag);

									if (flag) {
										balErrorCode = "0";

										additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
												: (additional_info + " | ")) + "Customer Previous Balance: " + userBalance
												+ " | Customer Updated Balance: "
												+ getUserBalance(jd.getNString("user_id"));

										balErrorMessage = "Successfully updated balance";
										updateBalTrxStatus(trx_id, "4", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

										String phone = getUserPhone(jd.getNString("user_id"));
										String email = getUserEmail(jd.getNString("user_id"));

										try {
//											send email
											if (NullPointerExceptionHandler.isNullOrEmpty(email)) {

											} else {
												
												if(commissionAmount==0.0) {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balanceTransferSuccess", phone, email, trx_id);
												} else {
													new UserDBOperations(weTopUpDS, configurations, logWriter)
													.sendEmail("balanceTransferWithCommissionSuccess", phone, email, trx_id);
												}
												
												updateEmailStatus(trx_id, "2");
											}
										} catch (Exception e) {
											e.printStackTrace();
										}
									} else {
										balErrorCode = "5";
										balErrorMessage = "Failed to update balance";
										updateBalTrxStatus(trx_id, "10", additional_info, trx_type, commissionRate,
												commissionAmount, creditAmount);

//											send email
									}
								}
							} else {
								balErrorCode = "-4";
								balErrorMessage = "Failed to fetch commission rate";
							}
						}
					}
					LogWriter.LOGGER.info("balErrorMessage : " + balErrorMessage);
					jsonEncoder.addElement("balErrorCode", balErrorCode);
					jsonEncoder.addElement("balErrorMessage", balErrorMessage);
				} catch (Exception e) {
					LogWriter.LOGGER.severe(e.getMessage());
					balErrorCode = "-3";
					balErrorMessage = "Failed to update balance";
					jsonEncoder.addElement("balErrorCode", balErrorCode);
					jsonEncoder.addElement("balErrorMessage", balErrorMessage);
				}
			} catch (SQLException e) {
				LogWriter.LOGGER.severe(e.getMessage());
				errorCode = "11";
				errorMessage = "SQL Exception";
			}
		} else {
			errorCode = "10";
			errorMessage = "Already updated.";
			JsonDecoder jd = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());

			jsonEncoder.addElement("trx_status", jd.getNString("trx_status"));
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	private boolean checkIfCardIsBlocked(String user_id, String card_number, String trx_id) {
		boolean retval = true;

		String sqlGuest = "SELECT count(*),? FROM cards_list where card_number = ? and guest_status = ?";
		String sqlUser = "SELECT count(*) FROM cards_list where user_id=? and card_number = ? and status = ?";

		String sql = "";

		if (user_id.equals("0")) {
			sql = sqlGuest;
		} else {
			sql = sqlUser;
		}

		int count = 0;
		try {
			weTopUpDS.prepareStatement(sql);
			card_number = card_number.toUpperCase();

			weTopUpDS.getPreparedStatement().setString(1, user_id);
			weTopUpDS.getPreparedStatement().setString(2, card_number);
			weTopUpDS.getPreparedStatement().setString(3, "1"); // status = 1 for unblocked cards

			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				count = rs.getInt(1);
			}
			if (count > 0) {
				retval = false;
			} else {
				insertIntoCardList(user_id, card_number, trx_id);
			}
			LogWriter.LOGGER.info("card_number : " + card_number);
			LogWriter.LOGGER.info("cardIsBlocked : " + retval);
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			LogWriter.LOGGER.severe("cardIsBlocked(): " + e.getMessage());
		}
		return retval;
	}

	public String insertIntoCardList(String user_id, String card_number, String trx_id) {

		String errorCode = "-1";

		if (NullPointerExceptionHandler.isNullOrEmpty(user_id)
				|| NullPointerExceptionHandler.isNullOrEmpty(card_number)) {
			errorCode = "5";
		} else {
			try {
				String sqlTransactionLog = "INSERT INTO cards_list (user_id,card_number,trx_id) VALUES (?,?,?)";

				try {
					weTopUpDS.prepareStatement(sqlTransactionLog, true);
					weTopUpDS.getPreparedStatement().setString(1, user_id);
					weTopUpDS.getPreparedStatement().setString(2, card_number);
					weTopUpDS.getPreparedStatement().setString(3, trx_id);

					weTopUpDS.execute();

					errorCode = "0";
					LogWriter.LOGGER.info("inserted into card_list.");
				} catch (SQLIntegrityConstraintViolationException de) {
					errorCode = "1";// : Same name Already exists
					LogWriter.LOGGER.info("Same TRX_ID Already exists.");
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

	private boolean updateEmailStatus(String trxID, String status) {
		boolean retval = false;
		String sqlUpdateUser = "UPDATE transaction_log set update_time=now(), notification_email=?  where trx_id=?";
		try {
			weTopUpDS.prepareStatement(sqlUpdateUser);
			weTopUpDS.getPreparedStatement().setString(1, status);
			weTopUpDS.getPreparedStatement().setString(2, trxID);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			LogWriter.LOGGER.info("updateEmailStatus(): email status for trxID : " + status);
			retval = true;
		} catch (SQLException e) {
			LogWriter.LOGGER.severe("updateEmailStatus(): " + e.getMessage());
		}
		return retval;
	}

	public Double calculateCommissionAmount(Double amount, Double commissionRate) {
		Double commissionAmount = 0.0;

		commissionAmount = amount * commissionRate / 100.0;

		return commissionAmount;
	}
	
	public JsonEncoder getUserFailedSummary(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String failedAmount = "10000";
		String failedCount = "100";
		
		String sql = "select count(*) as failedCount, ifnull(sum(amount),0) as failedAmount from transaction_log "
				+ "where user_id = ? and trx_status not in (2,3,4,6) and DATE(insert_time) = CURDATE() order by id desc";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				failedCount = rs.getString(1);
				failedAmount = rs.getString(2);
			}

			errorCode = "0";
			errorMessage = "fetched User Stock Refill summary.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			errorCode = "-3";
			errorMessage = "General exception : getUserStockRefillSummary.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

//		LogWriter.LOGGER.info("commissionRate :" + commissionRate);

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("failedAmount", "" + failedAmount);
		jsonEncoder.addElement("failedCount", "" + failedCount);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getUserStockRefillSummary(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String stockRefillAmount = "10000";
		String stockRefillCount = "100";
		
		String sql = "select count(*) as stockRefillCount, ifnull(sum(amount),0) as stockRefillAmount from transaction_log "
				+ "where user_id = ? and trx_status=2 and trx_type in (1) and  DATE(insert_time) = CURDATE() order by id desc";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				stockRefillCount = rs.getString(1);
				stockRefillAmount = rs.getString(2);
			}

			errorCode = "0";
			errorMessage = "fetched User Stock Refill summary.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			errorCode = "-3";
			errorMessage = "General exception : getUserStockRefillSummary.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

//		LogWriter.LOGGER.info("commissionRate :" + commissionRate);

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("stockRefillAmount", "" + stockRefillAmount);
		jsonEncoder.addElement("stockRefillCount", "" + stockRefillCount);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getUserTopupSummary(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String topupAmount = "10000";
		String topupCount = "100";
		
		String sql = "select count(*) as topupCount, ifnull(sum(amount),0) as topupAmount from transaction_log "
				+ "where user_id = ? and trx_status=2 and trx_type in (0) and  DATE(insert_time) = CURDATE() order by id desc";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				topupCount = rs.getString(1);
				topupAmount = rs.getString(2);
			}

			errorCode = "0";
			errorMessage = "fetched User Transfer summary.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			errorCode = "-3";
			errorMessage = "General exception : fetchUserTransferSummary.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

//		LogWriter.LOGGER.info("commissionRate :" + commissionRate);

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("topupAmount", "" + topupAmount);
		jsonEncoder.addElement("topupCount", "" + topupCount);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getUserTransferSummary(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String transferAmount = "10000";
		String transferCount = "100";
		
		String sql = "select count(*) as transferCount, ifnull(sum(amount),0) as transferAmount from transaction_log "
				+ "where user_id = ? and trx_status=2 and trx_type in (2,5) and  DATE(insert_time) = CURDATE() order by id desc";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				transferCount = rs.getString(1);
				transferAmount = rs.getString(2);
			}

			errorCode = "0";
			errorMessage = "fetched User Transfer summary.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			errorCode = "-3";
			errorMessage = "General exception : fetchUserTransferSummary.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

//		LogWriter.LOGGER.info("commissionRate :" + commissionRate);

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("transferAmount", "" + transferAmount);
		jsonEncoder.addElement("transferCount", "" + transferCount);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getUserOperationConfig(String id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String user_id = "";

		Double visaRate = 0.0;
		Double masterRate = 0.0;
		Double amexRate = 0.0;
		Double bkashRate = 0.0;
		Double cashRate = 0.0;
		Double transferRate = 0.0;
		Double minimumRechargeAmount = 500.0;
		Double minimumChargeAmount = 1.0;
		Double maximumChargeAmount = 10000.0;
		Double maximumTransferAmount = 5000.0;
		int maximumTransferCount = 100;
		Double maximumStockRefillAmount = 500.0;
		int maximumStockRefillCount = 10;
		Double maximumTopupAmount = 500.0;
		int maximumTopupCount = 10;
		Double maximumFailedAmount = 1500.0;
		Double maximumStockAmount = 1500.0;
		int maximumMsisdnFailedCount = 10;
		int maximumFailedCount = 10;
		int floor = 0;
		
		String sql = "SELECT t.cash_rate, t.visa_rate, t.master_rate, t.amex_rate, t.bkash_rate, t.minimum_transfer_amount, t.floor, t.transfer_rate, t.minimum_charge_amount, "
				+ "t.maximum_charge_amount, t.maximum_transfer_amount, t.maximum_transfer_count, "
				+ "t.maximum_stock_refill_amount, t.maximum_stock_refill_count, t.maximum_topup_amount, t.maximum_topup_count, t.maximum_failed_amount, t.maximum_failed_count, "
				+ "t.maximum_stock_amount, t.maximum_msisdn_failed_count, u.user_id "
				+ "FROM users_info u left join commission_configurations t on u.user_id=t.user_id where u.phone=?  or u.user_id=? order by u.created_at desc";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, msisdnNormalize(id));
			weTopUpDS.getPreparedStatement().setString(2, id);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				cashRate = rs.getDouble(1);
				visaRate = rs.getDouble(2);
				masterRate = rs.getDouble(3);
				amexRate = rs.getDouble(4);
				bkashRate = rs.getDouble(5);
				minimumRechargeAmount = rs.getDouble(6);
				floor = rs.getInt(7);
				transferRate = rs.getDouble(8);
				minimumChargeAmount = rs.getDouble(9);
				maximumChargeAmount = rs.getDouble(10);
				maximumTransferAmount = rs.getDouble(11);
				maximumTransferCount = rs.getInt(12);
				
				maximumStockRefillAmount = rs.getDouble(13);
				maximumStockRefillCount = rs.getInt(14);
				maximumTopupAmount = rs.getDouble(15);
				maximumTopupCount = rs.getInt(16);
				maximumFailedAmount = rs.getDouble(17);
				maximumFailedCount = rs.getInt(18);
				
				maximumStockAmount = rs.getDouble(19);
				maximumMsisdnFailedCount = rs.getInt(20);
				
				user_id = rs.getString(21);
			}

			errorCode = "0";
			errorMessage = "fetched UserOperationConfigs successfully.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			errorCode = "-3";
			errorMessage = "General exception : getUserOperationConfig.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

//		LogWriter.LOGGER.info("commissionRate :" + commissionRate);

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("cashRate", "" + cashRate);
		jsonEncoder.addElement("visaRate", "" + visaRate);
		jsonEncoder.addElement("masterRate", "" + masterRate);
		jsonEncoder.addElement("amexRate", "" + amexRate);
		jsonEncoder.addElement("bkashRate", "" + bkashRate);
		jsonEncoder.addElement("transferRate", "" + transferRate);
		jsonEncoder.addElement("minimumRechargeAmount", "" + minimumRechargeAmount);
		jsonEncoder.addElement("minimumChargeAmount", "" + minimumChargeAmount);
		jsonEncoder.addElement("maximumChargeAmount", "" + maximumChargeAmount);
		jsonEncoder.addElement("maximumTransferAmount", "" + maximumTransferAmount);
		jsonEncoder.addElement("maximumTransferCount", "" + maximumTransferCount);
		
		jsonEncoder.addElement("maximumStockRefillAmount", "" + maximumStockRefillAmount);
		jsonEncoder.addElement("maximumStockRefillCount", "" + maximumStockRefillCount);
		jsonEncoder.addElement("maximumTopupAmount", "" + maximumTopupAmount);
		jsonEncoder.addElement("maximumTopupCount", "" + maximumTopupCount);
		jsonEncoder.addElement("maximumFailedAmount", "" + maximumFailedAmount);
		jsonEncoder.addElement("maximumFailedCount", "" + maximumFailedCount);
		jsonEncoder.addElement("maximumStockAmount", "" + maximumStockAmount);
		jsonEncoder.addElement("maximumMsisdnFailedCount", "" + maximumMsisdnFailedCount);
		
		jsonEncoder.addElement("floor", "" + floor);
		
		jsonEncoder.addElement("user_id", "" + user_id);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder updateBalTrxStatus(String trx_id, String status, String additional_info, String trx_type,
			Double commissionRate, Double commissionAmount, Double creditAmount) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		LogWriter.LOGGER.info("UPDATE : " + trx_id + "	" + status + "	" + additional_info);

		String sql = "UPDATE `transaction_log` SET update_time=now(), bal_rec_status =?, additional_info = case when additional_info is null and ? is not null then ? when additional_info is not null and ? is not null then concat(additional_info,' | ',?) else additional_info end, commission_amount=?, credit_amount=?, commission_rate=? WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, status);
			weTopUpDS.getPreparedStatement().setString(2, additional_info);
			weTopUpDS.getPreparedStatement().setString(3, additional_info);
			weTopUpDS.getPreparedStatement().setString(4, additional_info);
			weTopUpDS.getPreparedStatement().setString(5, additional_info);
			weTopUpDS.getPreparedStatement().setString(6, commissionAmount+"");
			weTopUpDS.getPreparedStatement().setString(7, creditAmount+"");
			weTopUpDS.getPreparedStatement().setString(8, commissionRate+"");
			weTopUpDS.getPreparedStatement().setString(9, trx_id);
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

	public JsonEncoder updateBalTrxStatus(String trx_id, String status, String additional_info, String trx_type) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		LogWriter.LOGGER.info("UPDATE : " + trx_id + "	" + status + "	" + additional_info);

		String sql = "UPDATE `transaction_log` SET update_time=now(), bal_rec_status =?, additional_info = case when additional_info is null and ? is not null then ? when additional_info is not null and ? is not null then concat(additional_info,' | ',?) else additional_info end WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, status);
			weTopUpDS.getPreparedStatement().setString(2, additional_info);
			weTopUpDS.getPreparedStatement().setString(3, additional_info);
			weTopUpDS.getPreparedStatement().setString(4, additional_info);
			weTopUpDS.getPreparedStatement().setString(5, additional_info);
			weTopUpDS.getPreparedStatement().setString(6, trx_id);
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

	public String fetchMailBody(String action) {
		String mailbody = "";

		List<String> emptyList = new ArrayList<>();
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");

		List<String> templateInfo = this.configurations.getUserTemplates().containsKey(action)
				? this.configurations.getUserTemplates().get(action)
				: emptyList;

		mailbody = (String) templateInfo.get(4);

		return mailbody;
	}
	
	public String fetchSmsBody(String action) {
		String mailbody = "";

		List<String> emptyList = new ArrayList<>();
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		
		List<String> templateInfo = this.configurations.getUserTemplates().containsKey(action)
				? this.configurations.getUserTemplates().get(action)
				: emptyList;

		mailbody = (String) templateInfo.get(7);

		return mailbody;
	}

	public String getPaymentMethod(String payment_method) {

		if (payment_method.equals("0")) {
			payment_method = "from your Safety Stock.";
		} else if (payment_method.equals("1")) {
			payment_method = "via Lebupay";
		}
		return payment_method;
	}

	public String fetchMailSubject(String action) {
		List<String> emptyList = new ArrayList<>();
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");
		emptyList.add("");

		List<String> templateInfo = this.configurations.getUserTemplates().containsKey(action)
				? this.configurations.getUserTemplates().get(action)
				: emptyList;

		String subject = (String) templateInfo.get(2);

		return subject;
	}

	public JsonEncoder updatePaymentMethod(String trx_id, String payment_method) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		if (NullPointerExceptionHandler.isNullOrEmpty(trx_id)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sql = "UPDATE `transaction_log` SET update_time=now(), payment_method =? WHERE trx_id=?";

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

	public String getUserFromTrx(String trx_id) {
		String user_id = "";

		String sql = "SELECT user_id FROM topup_log WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, trx_id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				user_id = rs.getString(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return user_id;
	}

	public JsonEncoder fetchUserRates(String userID) {
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
		String topup_trx_id = "";
		String payment_method = "";
		String lp_trx_status = "";

		String errorCode = "-1";
		String errorMessage = "General error.";
		if (NullPointerExceptionHandler.isNullOrEmpty(userID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sql = "select\n" + "-- JSON_LENGTH(JSON_ARRAYAGG(t.top_up_status)) counter,\n"
					+ "-- GROUP_CONCAT(t.top_up_status ORDER BY t.top_up_status DESC SEPARATOR ',') gc,\n"
					+ "-- JSON_OBJECTAGG(t.top_up_status, t.retry_counter) jsn,\n"
					+ "-- JSON_ARRAYAGG(t.top_up_status) ary,\n" + "-- max(t.retry_counter) rc,\n"
					+ "-- max(t.top_up_status),\n" + "-- ,max(rp.max_attempt)\n"
					+ "--  ,min(t.user_id),max(t.user_id),min(rp.id),max(rp.id)\n"
					+ "max(t.user_id),max(t.operator),max(t.opType),max(t.payee_email),max(t.remarks),max(t.topup_trx_id),tx.trx_id,tx.user_trx_id, max(t.payee_phone),max(tx.amount),max(tx.trx_status),max(lp_trx_status),\n"
					+ "case when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) then '4' -- definite success\n"
					+	"	 when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('0')) then '11' -- processing\n"
					+ "	  when !JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) \n"
					+ "           and  (max(t.retry_counter)>=any_value(rp.max_attempt) \n"
					+ "                  or max(tx.trx_status) !=2)  then '10' -- definite fail\n"
					+ "	  else '11' -- processing\n" + " end topup_status,\n"
					+ "date_format(max(t.response_time), '%Y-%m-%d %H:%i:%S') as response_time ,max(tx.payment_method)\n"
					+ "\n" + "from topup_log t inner join transaction_log tx on t.trx_id=tx.trx_id \n"
					+ "left join retry_profile rp on tx_id order by response_time desc";
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, userID);

				ResultSet rs = weTopUpDS.executeQuery();
				if (rs.next()) {
					user_id = rs.getString(1);
					operator = rs.getString(2);
					opType = rs.getString(3);
					payee_email = rs.getString(4);
					remarks = rs.getString(5);
					topup_trx_id = rs.getString(6);
					payee_phone = rs.getString(9);
					amount = rs.getString(10);
					trx_status = rs.getString(11);
					lp_trx_status = rs.getString(12);
					top_up_status = rs.getString(13);
					response_time = rs.getString(14);
					payment_method = rs.getString(15);

					errorCode = "0";
					errorMessage = "getStatus successful.";
				}
				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("topup_trx_id",
				(NullPointerExceptionHandler.isNullOrEmpty(topup_trx_id) ? "" : topup_trx_id));
		jsonEncoder.addElement("trx_status", trx_status);
		jsonEncoder.addElement("lp_trx_status",
				(NullPointerExceptionHandler.isNullOrEmpty(lp_trx_status) ? "" : lp_trx_status));
		jsonEncoder.addElement("top_up_status", top_up_status);
		jsonEncoder.addElement("user_id", (NullPointerExceptionHandler.isNullOrEmpty(user_id) ? "" : user_id));
		jsonEncoder.addElement("payment_method", payment_method);
		jsonEncoder.addElement("payee_phone", payee_phone);
		jsonEncoder.addElement("amount", amount);
		jsonEncoder.addElement("operator", operator);
		jsonEncoder.addElement("opType", opType);
		jsonEncoder.addElement("payee_email",
				(NullPointerExceptionHandler.isNullOrEmpty(payee_email) ? "" : payee_email));
		jsonEncoder.addElement("remarks", (NullPointerExceptionHandler.isNullOrEmpty(remarks) ? "" : remarks));
		jsonEncoder.addElement("response_time", response_time);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getTopUpStatus(String userID, String trxID, String userTrxID) {
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
		String topup_trx_id = "";
		String payment_method = "";
		String lp_trx_status = "";

		String errorCode = "-1";
		String errorMessage = "General error.";
		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || (NullPointerExceptionHandler.isNullOrEmpty(trxID)
				&& NullPointerExceptionHandler.isNullOrEmpty(userTrxID))) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sql = "select\n" + "-- JSON_LENGTH(JSON_ARRAYAGG(t.top_up_status)) counter,\n"
					+ "-- GROUP_CONCAT(t.top_up_status ORDER BY t.top_up_status DESC SEPARATOR ',') gc,\n"
					+ "-- JSON_OBJECTAGG(t.top_up_status, t.retry_counter) jsn,\n"
					+ "-- JSON_ARRAYAGG(t.top_up_status) ary,\n" + "-- max(t.retry_counter) rc,\n"
					+ "-- max(t.top_up_status),\n" + "-- ,max(rp.max_attempt)\n"
					+ "--  ,min(t.user_id),max(t.user_id),min(rp.id),max(rp.id)\n"
					+ "max(t.user_id),max(t.operator),max(t.opType),max(t.payee_email),max(t.remarks),max(t.topup_trx_id),tx.trx_id,tx.user_trx_id, max(t.payee_phone),max(tx.amount),max(tx.trx_status),max(lp_trx_status),\n"
					+ "case when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) then '4' -- definite success\n"
					+	"	 when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('0')) then '11' -- processing\n"
					+ "	  when !JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) \n"
					+ "           and  (max(t.retry_counter)>=any_value(rp.max_attempt) \n"
					+ "                  or max(tx.trx_status) !=2)  then '10' -- definite fail\n"
					+ "	  else '11' -- processing\n" + " end topup_status,\n"
					+ "date_format(max(t.response_time), '%Y-%m-%d %H:%i:%S') as response_time ,max(tx.payment_method)\n"
					+ "\n" + "from topup_log t inner join transaction_log tx on t.trx_id=tx.trx_id \n"
					+ "left join retry_profile rp on tx.retry_profile = rp.id\n" + "where tx.user_id=? and "
					+ (NullPointerExceptionHandler.isNullOrEmpty(trxID) ? ("tx.user_trx_id='" + userTrxID)
							: ("tx.trx_id='" + trxID))
					+ "' -- CHANGE\n" + "group by tx.trx_id order by response_time desc";
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, userID);

				ResultSet rs = weTopUpDS.executeQuery();
				if (rs.next()) {
					user_id = rs.getString(1);
					operator = rs.getString(2);
					opType = rs.getString(3);
					payee_email = rs.getString(4);
					remarks = rs.getString(5);
					topup_trx_id = rs.getString(6);
					trxID = rs.getString(7);
					userTrxID = rs.getString(8);
					payee_phone = rs.getString(9);
					amount = rs.getString(10);
					trx_status = rs.getString(11);
					lp_trx_status = rs.getString(12);
					top_up_status = rs.getString(13);
					response_time = rs.getString(14);
					payment_method = rs.getString(15);

					errorCode = "0";
					errorMessage = "getStatus successful.";
				}
				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("trx_id", (NullPointerExceptionHandler.isNullOrEmpty(trxID) ? "" : trxID));
		jsonEncoder.addElement("user_trx_id", (NullPointerExceptionHandler.isNullOrEmpty(userTrxID) ? "" : userTrxID));
		jsonEncoder.addElement("topup_trx_id",
				(NullPointerExceptionHandler.isNullOrEmpty(topup_trx_id) ? "" : topup_trx_id));
		jsonEncoder.addElement("trx_status", trx_status);
		jsonEncoder.addElement("lp_trx_status",
				(NullPointerExceptionHandler.isNullOrEmpty(lp_trx_status) ? "" : lp_trx_status));
		jsonEncoder.addElement("top_up_status", top_up_status);
		jsonEncoder.addElement("user_id", (NullPointerExceptionHandler.isNullOrEmpty(user_id) ? "" : user_id));
		jsonEncoder.addElement("payment_method", payment_method);
		jsonEncoder.addElement("payee_phone", payee_phone);
		jsonEncoder.addElement("amount", amount);
		jsonEncoder.addElement("operator", operator);
		jsonEncoder.addElement("opType", opType);
		jsonEncoder.addElement("payee_email",
				(NullPointerExceptionHandler.isNullOrEmpty(payee_email) ? "" : payee_email));
		jsonEncoder.addElement("remarks", (NullPointerExceptionHandler.isNullOrEmpty(remarks) ? "" : remarks));
		jsonEncoder.addElement("response_time", response_time);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getFileTopupSummary(String userID, String fileID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String errorCode = "-1";
		String errorMessage = "General error.";
		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(fileID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			
			JsonDecoder jd =  new JsonDecoder(getFileCountSummary(fileID,userID).getJsonObject().toString());
			String total_msisdn = jd.getNString("totalCount");
			String fileName = jd.getNString("fileName");
			String updatedFileName = jd.getNString("updatedFileName");
			String insertTime = jd.getNString("insertTime");
			String total_amount = jd.getNString("amount");
			String success_msisdn = jd.getNString("validCount");
			
			String successCount = "0";
			String failCount = "0";
			String onProcessCount = "0";
			
			jsonEncoder.addElement("totalEntry", total_msisdn);
			jsonEncoder.addElement("fileName", fileName);
			jsonEncoder.addElement("updatedFileName", updatedFileName);
			jsonEncoder.addElement("insertTime", insertTime);
			jsonEncoder.addElement("totalAmount", total_amount);
			jsonEncoder.addElement("validEntry", success_msisdn);
			
			String sql = "select t0.topup_status, count(t0.topup_status) as count from (\n" + 
					"select \n" + 
					"case when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) then '4' -- definite success\n" + 
					"	 when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('0')) then '11' -- processing\n" + 
					"	 when !JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) \n" + 
					"           and  (max(t.retry_counter)>=any_value(rp.max_attempt) \n" + 
					"                  or (max(tx.trx_status) !=2 and max(tx.trx_status) !=6))  then '10' -- definite fail\n" + 
					"	  else '11' -- processing  \n" + 
					"      end topup_status\n" + 
					" from topup_log t inner join transaction_log tx on t.trx_id=tx.trx_id \n" + 
					"left join retry_profile rp on tx.retry_profile = rp.id where tx.user_id=? and tx.ref_file_id=? -- CHANGE \n" + 
					"group by tx.trx_id) t0 group by t0.topup_status";
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, userID);
				weTopUpDS.getPreparedStatement().setString(2, fileID);

				ResultSet rs = weTopUpDS.executeQuery();
				while(rs.next()) {
					if(rs.getInt(1)==4) {
						successCount = rs.getString(2);
					} else if(rs.getInt(1)==11) {
						onProcessCount = rs.getString(2);
					} else if(rs.getInt(1)==10) {
						failCount = rs.getString(2);
					}

					
				}
				jsonEncoder.addElement("successCount", successCount);
				jsonEncoder.addElement("failCount", failCount);
				jsonEncoder.addElement("onProcessCount", onProcessCount);
				errorCode = "0";
				errorMessage = "getStatus successful.";
				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder getFileTopupHistory(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
		String errorCode = "-1";
		String errorMessage = "General error.";
		String fileHistory = "";
		if (NullPointerExceptionHandler.isNullOrEmpty(userID)) {
			errorCode = "5"; 
			errorMessage = "Missing one or more parameters.";
		} else {
			String sql = "SELECT id, file_name, updated_file_name, date_format(insert_time, '%Y-%m-%d %H:%i:%S'), date_format(upload_time, '%Y-%m-%d %H:%i:%S'), status, estimated_upload_time, "
					+ "comments FROM topup_file_info where user_id =? order by insert_time desc";
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, userID);
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
					fileHistory += "\"" + rs.getString(1) + "\"" + ",";
					fileHistory += "\"" + rs.getString(2) + "\"" + ",";
					fileHistory += "\"" + rs.getString(3) + "\"" + ",";
					fileHistory += "\"" + rs.getString(4) + "\"" + ",";
					fileHistory += "\"" + rs.getString(5) + "\"" + ",";
					fileHistory += "\"" + rs.getString(6) + "\"" + ",";
					fileHistory += "\"" + rs.getString(7) + "\"" + ",";
					fileHistory += "\"" + rs.getString(8) + "\"" + "|";
				}
				int lio = fileHistory.lastIndexOf("|");
				if (lio > 0)
					fileHistory = fileHistory.substring(0, lio);

				errorCode = "0";
				errorMessage = "fetched file topup history successfully.";

				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("fileHistory", fileHistory);
		

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public String fetchAccessKey(String operator, String test) {
		String accessKey = "";
		int accessFlag = 1;

		// Operator: 0=Airtel ; 1=Robi ; 2=GrameenPhone ; 3=Banglalink ; 4=Teletalk

		if (test.equals("Y")) {

		} else {
			if (operator.equals("0") || operator.equals("1")) {
				accessFlag = 2;
			} else if (operator.equals("10")) {
				accessFlag = 5;
			} else {
				accessFlag = 3;
			}
		}

		String sql = "SELECT access_key FROM lebupay_access_key where id = ?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, accessFlag);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				accessKey = rs.getString(1);
			}
			rs.close();
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
		String amount = "";
		String receiver_phone = "";
		String trx_status = "";
		String bal_rec_status = "";
		String insert_time = "";
		String update_time = "";
		String additional_info = "";
		String payment_method = "";
		String card_brand = "";
		String card_number = "";
		String bank = "";
		String bkash_payment_number = "";
		String billing_name = "";
		String trx_type = "";
		Double commission_amount = 0.0;
		Double credit_amount = 0.0;
		Double commission_rate = 0.0;
		String notification_email = "";
		String ref_trx_id = "";

		String source = "";
		String LP_trx_status = "";
		String bin_issuer_bank = "";
		String bin_issuer_country = "";
		
		String errorCode = "-1";
		String errorMessage = "General error.";

		String sql = "SELECT user_id, trx_id, amount, receiver_phone, trx_status, bal_rec_status, date_format(insert_time, '%Y-%m-%d %H:%i:%S'),date_format(update_time, '%Y-%m-%d %H:%i:%S') , additional_info, "
				+ "payment_method, card_brand, card_number, bank, bkash_payment_number, billing_name, trx_type,commission_amount, credit_amount, commission_rate,notification_email,ref_trx_id,source, LP_trx_status, bin_issuer_bank, bin_issuer_country FROM transaction_log where trx_id=?";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, trx_id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				user_id = rs.getString(1);
				trx_id = rs.getString(2);
				amount = rs.getString(3);
				receiver_phone = rs.getString(4);
				trx_status = rs.getString(5);
				bal_rec_status = rs.getString(6);
				insert_time = rs.getString(7);
				update_time = rs.getString(8);
				additional_info = rs.getString(9);
				payment_method = rs.getString(10);
				card_brand = rs.getString(11);
				card_number = rs.getString(12);
				bank = rs.getString(13);
				bkash_payment_number = rs.getString(14);
				billing_name = rs.getString(15);
				trx_type = rs.getString(16);
				commission_amount = rs.getDouble(17);
				credit_amount = rs.getDouble(18);
				commission_rate = rs.getDouble(19);
				notification_email = rs.getString(20);
				ref_trx_id = rs.getString(21);

				source = rs.getString(22);
				LP_trx_status = rs.getString(23);
				bin_issuer_bank = rs.getString(24);
				bin_issuer_country = rs.getString(25);
				
				errorCode = "0";
				errorMessage = "getStatus successful.";
			}
			rs.close();
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
		jsonEncoder.addElement("amount", amount);
		jsonEncoder.addElement("receiver_phone",NullPointerExceptionHandler.isNullOrEmpty(receiver_phone) ? "" : receiver_phone);
		jsonEncoder.addElement("trx_status", trx_status);
		jsonEncoder.addElement("bal_rec_status", bal_rec_status);
		jsonEncoder.addElement("insert_time",NullPointerExceptionHandler.isNullOrEmpty(insert_time) ? "" : insert_time);
		jsonEncoder.addElement("update_time",NullPointerExceptionHandler.isNullOrEmpty(update_time) ? "" : update_time);
		jsonEncoder.addElement("additional_info",NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? "" : additional_info);
		jsonEncoder.addElement("payment_method", payment_method);
		jsonEncoder.addElement("card_brand", NullPointerExceptionHandler.isNullOrEmpty(card_brand) ? "" : card_brand);
		jsonEncoder.addElement("card_number",NullPointerExceptionHandler.isNullOrEmpty(card_number) ? "" : card_number);
		jsonEncoder.addElement("bank", NullPointerExceptionHandler.isNullOrEmpty(bank) ? "" : bank);
		jsonEncoder.addElement("bkash_payment_number",NullPointerExceptionHandler.isNullOrEmpty(bkash_payment_number) ? "" : bkash_payment_number);
		jsonEncoder.addElement("billing_name",NullPointerExceptionHandler.isNullOrEmpty(billing_name) ? "" : billing_name);
		jsonEncoder.addElement("commission_amount", "" + commission_amount);
		jsonEncoder.addElement("credit_amount", "" + credit_amount);
		jsonEncoder.addElement("commission_rate", "" + commission_rate);
		jsonEncoder.addElement("notification_email", "" + notification_email);
		jsonEncoder.addElement("trx_type", trx_type);
		jsonEncoder.addElement("ref_trx_id", NullPointerExceptionHandler.isNullOrEmpty(ref_trx_id) ? "" : ref_trx_id);
		
		jsonEncoder.addElement("source", NullPointerExceptionHandler.isNullOrEmpty(source) ? "" : source);
		jsonEncoder.addElement("LP_trx_status", NullPointerExceptionHandler.isNullOrEmpty(LP_trx_status) ? "" : LP_trx_status);
		jsonEncoder.addElement("bin_issuer_bank", NullPointerExceptionHandler.isNullOrEmpty(bin_issuer_bank) ? "" : bin_issuer_bank);
		jsonEncoder.addElement("bin_issuer_country", NullPointerExceptionHandler.isNullOrEmpty(bin_issuer_country) ? "" : bin_issuer_country);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder getSingleTopUpTransaction(String trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String user_id = "";
		String payee_phone = "";
		String amount = "";
		String operator = "";
		String opType = "";
		String payee_email = "";
		String remarks = "";
		String errorCode = "-1";
		String errorMessage = "General error.";

		String sql = "SELECT user_id, payee_phone, amount, operator, opType, payee_email, remarks FROM topup_log WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, trx_id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				user_id = rs.getString(1);
				payee_phone = rs.getString(2);
				amount = rs.getString(3);
				operator = rs.getString(4);
				opType = rs.getString(5);
				payee_email = rs.getString(6);
				remarks = rs.getString(7);
				errorCode = "0";
				errorMessage = "getStatus successful.";
			}
			rs.close();
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

	public JsonEncoder getAccessKey(String trx_id, String test) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String operator = "";
		String accessKey = "";
		String errorCode = "-1";
		String errorMessage = "General error.";

		String sql = "SELECT operator FROM topup_log WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, trx_id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				operator = rs.getString(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();

			accessKey = fetchAccessKey(operator, test);

			if (accessKey.equals("")) {
				errorCode = "5";
				errorMessage = "fetchAccessKey failed.";
			} else {
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

	public JsonEncoder getFileUploadStatus(String user_id, String fileID, String extended) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String file_name = "";
		String status = "-5";
		String created = "";
		String uploaded = "";
		String estimated_upload_time = "";
		String comments = "";

		String statusQuery = "SELECT status from topup_file_info where id = ?";
		String allQuery = "SELECT file_name, user_id, insert_time, upload_time, status, estimated_upload_time, comments from topup_file_info where id = ? and user_id=?";
		String query = "";
		boolean fetchAll = false;

		if (!NullPointerExceptionHandler.isNullOrEmpty(extended)) {
			if (extended.equals("1")) {
				query = allQuery;
				fetchAll = true;
			} else {
				query = statusQuery;
			}
		} else {
			query = statusQuery;
		}

		if (!NullPointerExceptionHandler.isNullOrEmpty(fileID)) {
//			LogWriter.LOGGER.info("query : " + query);
			String errorCode = "-1";
			String errorMessage = "not initiated.";

			try {
				try {
					LogWriter.LOGGER.info("fetching upload status.");
					weTopUpDS.prepareStatement(query);
					weTopUpDS.getPreparedStatement().setString(1, fileID);
					weTopUpDS.getPreparedStatement().setString(2, user_id);

					ResultSet rs = weTopUpDS.executeQuery();
					while (rs.next()) {
						status = rs.getString("status");
						if (fetchAll) {
							user_id = rs.getString("user_id");
							file_name = rs.getString("file_name");
							created = rs.getString("insert_time");
							uploaded = rs.getString("upload_time");
							estimated_upload_time = rs.getString("estimated_upload_time");
							comments = rs.getString("comments");
						}
					}
					LogWriter.LOGGER.info("fetched upload status : " + status);

					weTopUpDS.closeResultSet();
					weTopUpDS.closePreparedStatement();
					errorCode = "0";// :Successfully
					errorMessage = "Successfully fetched.";
				} catch (SQLException e) {
					errorCode = "11";// :Inserting parameters failed
					errorMessage = "SQLException.";
					e.printStackTrace();
					LogWriter.LOGGER.severe("SQLException" + e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
					errorCode = "10"; // :other Exception
					errorMessage = "General exception.";
					e.printStackTrace();
				}
			} finally {
				if (weTopUpDS.getConnection() != null) {
					try {
						weTopUpDS.closePreparedStatement();
						// weTopUpDS.getConnection().close();
					} catch (SQLException e) {
						errorCode = "-4";
						errorMessage = "connection close Exception.";
						e.printStackTrace();
						LogWriter.LOGGER.severe(e.getMessage());
					}
				}
			}
			jsonEncoder.addElement("ErrorCode", errorCode);
			jsonEncoder.addElement("ErrorMessage", errorMessage);
			jsonEncoder.addElement("status", status);
			if (fetchAll) {
				user_id = (NullPointerExceptionHandler.isNullOrEmpty(user_id) ? "" : user_id);
				created = (NullPointerExceptionHandler.isNullOrEmpty(created) ? "" : created);
				uploaded = (NullPointerExceptionHandler.isNullOrEmpty(uploaded) ? "" : uploaded);
				estimated_upload_time = (NullPointerExceptionHandler.isNullOrEmpty(estimated_upload_time) ? ""
						: estimated_upload_time);
				comments = (NullPointerExceptionHandler.isNullOrEmpty(comments) ? "" : comments);

				jsonEncoder.addElement("fileID", fileID);
				jsonEncoder.addElement("file_name", file_name);
				jsonEncoder.addElement("user_id", user_id);
				jsonEncoder.addElement("insert_date", created);
				jsonEncoder.addElement("uploaded_date", uploaded);
				jsonEncoder.addElement("estimated_upload_time", estimated_upload_time);
				jsonEncoder.addElement("comments", comments);
			}

			jsonEncoder.buildJsonObject();
			return jsonEncoder;
		} else {
			jsonEncoder.addElement("ErrorCode", "5");
			jsonEncoder.addElement("ErrorMessage", "Missing one or more parameters.");

			jsonEncoder.buildJsonObject();
			return jsonEncoder;
		}
	}

	public JsonEncoder fetchRetailerList(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String retailer_list = "";

		String sql = "SELECT u.user_name,u.phone,u.user_email,u.address,t.cash_rate,date_format(u.created_at, '%Y-%m-%d %H:%i:%S'),u.balance,u.dp_img,u.doc_img_01,u.doc_img_02,u.doc_img_03 FROM users_info u left join commission_configurations t on u.user_id=t.user_id where distributor_id=? order by u.created_at desc";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				// user_name, phone, user_email, address, cash_rate, date_created_at,
				// balance, dp_img, doc_img_01, doc_img_02, doc_img_03
				retailer_list += "\"" + rs.getString(1) + "\"" + ";";
				retailer_list += "\"" + rs.getString(2) + "\"" + ";";
				retailer_list += "\"" + rs.getString(3) + "\"" + ";";
				retailer_list += "\"" + rs.getString(4) + "\"" + ";";
				retailer_list += "\"" + rs.getString(5) + "\"" + ";";
				retailer_list += "\"" + rs.getString(6) + "\"" + ";";
				retailer_list += "\"" + rs.getString(7) + "\"" + ";";
				retailer_list += "\"" + rs.getString(8) + "\"" + ";";
				retailer_list += "\"" + rs.getString(9) + "\"" + ";";
				retailer_list += "\"" + rs.getString(10) + "\"" + ";";
				retailer_list += "\"" + rs.getString(11) + "\"" + "|";
			}
			int lio = retailer_list.lastIndexOf("|");
			if (lio > 0)
				retailer_list = retailer_list.substring(0, lio);

			errorCode = "0";
			errorMessage = "fetched retailer list successfully.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("retailer list :" + retailer_list);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("retailer_list", retailer_list);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder fetchShadowOpBalance() {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String balance_list = "";

		String sql = "SELECT operator_name, operator_balance, log_time FROM shadow_balance_info";

		try {
			weTopUpDS.prepareStatement(sql);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				balance_list += "\"" + rs.getString(1) + "\"" + ";";
				balance_list += "\"" + rs.getString(2) + "\"" + ";";
				balance_list += "\"" + rs.getString(3) + "\"" + "|";
			}
			int lio = balance_list.lastIndexOf("|");
			if (lio > 0)
				balance_list = balance_list.substring(0, lio);

			errorCode = "0";
			errorMessage = "fetched balance_list successfully.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		

		LogWriter.LOGGER.info("shadow_balance_list :" + balance_list);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("shadow_balance_list", balance_list);
		jsonEncoder.addElement("userStock", fetchTotalUserStock());
		

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchAllBalance(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String balance_list = "";

		if(checkIfManagerialAdmin(userID)) {
			String sql = "SELECT b.id, b.operator_name, b.operator_balance, b.log_time as op_log_time, s.operator_balance as shadow_balance, s.log_time as shadow_log_time FROM balance_info b left join shadow_balance_info s on b.id=s.id order by b.id asc";

			try {
				weTopUpDS.prepareStatement(sql);
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
					balance_list += "\"" + rs.getString(1) + "\"" + ";";
					balance_list += "\"" + rs.getString(2) + "\"" + ";";
					balance_list += "\"" + rs.getString(3) + "\"" + ";";
					balance_list += "\"" + rs.getString(4) + "\"" + ";";
					balance_list += "\"" + rs.getString(5) + "\"" + ";";
					balance_list += "\"" + rs.getString(6) + "\"" + "|";
				}
				int lio = balance_list.lastIndexOf("|");
				if (lio > 0)
					balance_list = balance_list.substring(0, lio);

				errorCode = "0";
				errorMessage = "fetched balance_list successfully.";

				rs.close();
				weTopUpDS.closePreparedStatement();
				jsonEncoder.addElement("userStock", fetchTotalUserStock());
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		} else {
			errorCode = "-5";
			errorMessage = "User not Authorized.";
		}
		
		
		

		LogWriter.LOGGER.info("shadow_balance_list :" + balance_list);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("shadow_balance_list", balance_list);
		
		

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	public JsonEncoder allocateShadowOpBalance(String userID, String operator, String amount) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		String sql = "update shadow_balance_info set operator_balance=operator_balance+?, log_time=now() where id=? and ((select operator_balance from balance_info where id=?)-operator_balance)>=?";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(operator) || NullPointerExceptionHandler.isNullOrEmpty(amount)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			if(checkIfManagerialAdmin(userID)) {
				String sqlProc = "{call operator_balance_update_proc()}";
				
				try {
					weTopUpDS.prepareCall(sqlProc);
					weTopUpDS.getCallableStatement().execute();
					weTopUpDS.closeCallableStatement();
				
					weTopUpDS.prepareStatement(sql,true);
					weTopUpDS.getPreparedStatement().setString(1, amount);
					weTopUpDS.getPreparedStatement().setString(2, operator);
					weTopUpDS.getPreparedStatement().setString(3, operator);
					weTopUpDS.getPreparedStatement().setString(4, amount);
					
					long count = weTopUpDS.executeUpdate();
					weTopUpDS.closePreparedStatement();
					if(count>0) {
						errorCode = "0";
						errorMessage = "allocation successful.";
						
						// insert log
						insertShadowBalanceLog( userID, operator, amount);
					} else {
						errorCode = "10";
						errorMessage = "allocation failed";
					}				
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
					errorCode = "11";
					errorMessage = "SQL Exception";
				}
			} else {
				errorCode = "-5";
				errorMessage = "User not Authorized.";
			}
			
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public Boolean insertShadowBalanceLog(String userID, String operator, String amount) {
		Boolean insertFlag = false;
		
		String sqlQuery = "INSERT INTO shadow_balance_log (operator, amount, log_time, action_by) VALUES (?,?,now(),?)";
		try {
			weTopUpDS.prepareStatement(sqlQuery, true);
			weTopUpDS.getPreparedStatement().setString(1, operator);
			weTopUpDS.getPreparedStatement().setString(2, amount);
			weTopUpDS.getPreparedStatement().setString(3, userID);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			insertFlag = true;
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		return insertFlag;
	}
	
	public JsonEncoder checkForUpdates(String userID, String appVersion) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String isCritical = "0";
		String version = "";
		String url = "";
		String whatsNew = "";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(appVersion)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sql = "SELECT json_contains(JSON_ARRAYAGG(json_array(isCritical)),json_array(01)) as is_critical, max(version) as version, max(url) as url, max(whats_new) as whatsNew FROM app_version where version>?";
	
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, appVersion);
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
//					is_critical, version, url
					isCritical = rs.getString(1);
					version = rs.getString(2);
					url = rs.getString(3);
					whatsNew = rs.getString(4);
				}
				errorCode = "0";
				errorMessage = "checked update successfully.";
	
				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}
		
		LogWriter.LOGGER.info("isCritical :" + isCritical
				+ "\nversion :" + version
				+ "\nurl :" + url);
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("isCritical", NullPointerExceptionHandler.isNullOrEmpty(isCritical)?"0":isCritical);
		jsonEncoder.addElement("version", NullPointerExceptionHandler.isNullOrEmpty(version)?"":version);
		jsonEncoder.addElement("url", NullPointerExceptionHandler.isNullOrEmpty(url)?"":url);
		jsonEncoder.addElement("whatsNew", NullPointerExceptionHandler.isNullOrEmpty(whatsNew)?"":whatsNew);
		
		

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchOpBalance() {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String balance_list = "";

		String sql = "SELECT operator_name, operator_balance, log_time FROM balance_info";

		try {
			weTopUpDS.prepareStatement(sql);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				balance_list += "\"" + rs.getString(1) + "\"" + ";";
				balance_list += "\"" + rs.getString(2) + "\"" + ";";
				balance_list += "\"" + rs.getString(3) + "\"" + "|";
			}
			int lio = balance_list.lastIndexOf("|");
			if (lio > 0)
				balance_list = balance_list.substring(0, lio);

			errorCode = "0";
			errorMessage = "fetched balance_list successfully.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		

		LogWriter.LOGGER.info("balance_list :" + balance_list);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("balance_list", balance_list);
		jsonEncoder.addElement("userStock", fetchTotalUserStock());
		

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public String fetchTotalUserStock() {
		String userStock = "";
		
		String sql = "SELECT sum(balance) as UserStock FROM users_info";

		try {
			weTopUpDS.prepareStatement(sql);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				userStock = rs.getString(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("userStock :" + userStock);
		return userStock;
	}
	
	public JsonEncoder fetchDownloadStatus(String userID, String downloadID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String downloadStatus = "";
		String fileName = "";

		String sql = "select status, file_name from file_dump_query where id = ? and user_id = ?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, downloadID);
			weTopUpDS.getPreparedStatement().setString(2, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				downloadStatus = rs.getString(1);
				fileName = rs.getString(2);
			}

			errorCode = "0";
			errorMessage = "fetched download status successfully.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("downloadStatus :" + downloadStatus);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("downloadStatus", downloadStatus);
		jsonEncoder.addElement("fileName", NullPointerExceptionHandler.isNullOrEmpty(fileName) ? "" : fileName);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder fetchUserCardList(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String card_list = "";

		String sql = "SELECT user_id, card_number, region, status, guest_status, date_format(insert_time, '%Y-%m-%d %H:%i:%S') FROM cards_list where user_id=? order by id desc";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				// user_name, phone, user_email, address, cash_rate, date_created_at,
				// balance, dp_img, doc_img_01, doc_img_02, doc_img_03
				card_list += "\"" + rs.getString(1) + "\"" + ";";
				card_list += "\"" + rs.getString(2) + "\"" + ";";
				card_list += "\"" + rs.getString(3) + "\"" + ";";
				card_list += "\"" + rs.getString(4) + "\"" + ";";
				card_list += "\"" + rs.getString(5) + "\"" + ";";
				card_list += "\"" + rs.getString(6) + "\"" + "|";
			}
			int lio = card_list.lastIndexOf("|");
			if (lio > 0)
				card_list = card_list.substring(0, lio);

			errorCode = "0";
			errorMessage = "fetched card list successfully.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("card list :" + card_list);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("card_list", card_list);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchUserCardCounts(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String pending_card_count = "0";

		String sql = "select count(*) from cards_list where user_id=? and status=-1 order by id desc";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				pending_card_count = rs.getString(1);
				
			}
			
			errorCode = "0";
			errorMessage = "fetched card list successfully.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("pending_card_count :" + pending_card_count);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("pending_card_count", NullPointerExceptionHandler.isNullOrEmpty(pending_card_count)?"0":pending_card_count);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchPendingTrxUser(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String pending_trx_list = "";

		String sql = "select c.trx_id, t.amount, t.trx_type, c.status, t.insert_time, c.json_req from cards_list c left join transaction_log t on c.trx_id=t.trx_id where c.user_id=? order by c.id desc";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			while (rs.next()) {
				// trx_id, amount, trx_type, status, insert_time, json_req
				pending_trx_list += "\"" + rs.getString(1) + "\"" + ";";
				pending_trx_list += "\"" + rs.getString(2) + "\"" + ";";
				pending_trx_list += "\"" + rs.getString(3) + "\"" + ";";
				pending_trx_list += "\"" + rs.getString(4) + "\"" + ";";
				pending_trx_list += "\"" + rs.getString(5) + "\"" + ";";
				pending_trx_list += "\"" + rs.getString(6) + "\"" + "|";
			}
			int lio = pending_trx_list.lastIndexOf("|");
			if (lio > 0)
				pending_trx_list = pending_trx_list.substring(0, lio);

			errorCode = "0";
			errorMessage = "fetched card list successfully.";

			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("pending_trx_list :" + pending_trx_list);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("pending_trx_list", pending_trx_list);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder fetchPendingTrxAdmin(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		JsonEncoder jsonEncoder2 = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String pending_trx_list = "";

		if (checkIfAdmin(userID)) {
			String sql = "SELECT CONCAT(user_id,'|',card_number), GROUP_CONCAT(CONCAT(json_req, ';',guest_status,';',update_time) SEPARATOR '|') jsonArray FROM cards_list where status=? GROUP BY user_id,card_number order by user_id desc";

			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setInt(1, 0);
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
//					LogWriter.LOGGER.info("here");
					jsonEncoder2.addElement(rs.getString(1), NullPointerExceptionHandler.isNullOrEmpty(rs.getString(2))?"{}":rs.getString(2));
				}
				jsonEncoder2.buildJsonObject();
				errorCode = "0";
				errorMessage = "fetched card list successfully.";
				jsonEncoder.addElement("pending_trx_list", jsonEncoder2.getJsonObject().toString());
				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		} else {
			errorCode = "25";
			errorMessage = "User is not authorized to perform this action.";
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder fetchStocksHistory(String userID, String startDate, String endDate) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String trx_history = "";
		String sql = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(userID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String stockConfigurations = fetchUserStockConfigurations(userID);
			// check if Stock history allowed for user
			// 0 = inactive; 1 = active;		bitwise	:		0 = visibility;	1 = history;	2 = topup;	3 = refill;	4 = transfer;
			if (stockConfigurations.charAt(1) == '0') {
				LogWriter.LOGGER.info("Stock History not permitted.");
				errorCode = "0";
				errorMessage = "fetched Stock transaction history successfully.";
			} else {
				if (NullPointerExceptionHandler.isNullOrEmpty(startDate)) {
					startDate = "2018-01-01";
				}
				if (NullPointerExceptionHandler.isNullOrEmpty(endDate)) {
					endDate = "2050-01-01";
				}
				String userPhone = getUserPhone(userID);
				userPhone = NullPointerExceptionHandler.isNullOrEmpty(userPhone)?"-1":userPhone;
				sql = "SELECT t.user_id, t.trx_id, ifnull(t.ref_trx_id,'') as 'Ref. TrxID', " + 
						"case " + 
						"	when t.user_id=? and t.trx_type=2 then t.amount " + 
						"   when t.user_id!=? and t.trx_type=2 then (t.amount+t.commission_amount) " + 
						"	when t.user_id=? and t.trx_type=5 then t.amount " + 
						"   when t.user_id!=? and t.trx_type=5 then (t.amount-t.commission_amount) " + 
						"   else (t.amount+t.commission_amount) " + 
						"end as amount, " + 
						"case " + 
						"	when t.trx_type=0 then 'Stock Deduction for Topup' " + 
						"	when t.trx_type=1 then 'Stock Refill' " + 
						"   when t.trx_type=2 then 'Stock refill from Distributor' " + 
						"   when t.trx_type=3 then 'Stock Refund' " + 
						"   when t.trx_type=4 then 'Stock deduction' " + 
						"   when t.trx_type=5 then 'Stock transfer' " + 
						"end as Purpose, " + 
						"case " + 
						"	when t.trx_status=1 then 'Cancelled' " + 
						"	when t.trx_status=2 then 'Successful' " + 
						"	when t.trx_status=3 then 'Refunded' " + 
						"   when t.trx_status=4 then 'Refunded after deduction' " + 
						"   when t.trx_status=5 then 'Halted' " + 
						"end as 'Status Message',  " + 
						"t.trx_type, t.trx_status, t.payment_method, ifnull(u.phone,'n/a') as Sender, t.receiver_phone as Receiver, date_format(t.insert_time, '%Y-%m-%d %H:%i:%S') as Date " + 
						"FROM transaction_log t left join users_info u on t.user_id=u.user_id where t.trx_status not in (0,6) and (t.user_id=? or (t.trx_type=2 and t.receiver_phone=?) or (t.trx_type=5 and t.receiver_phone=?)) and  " + 
						"( CASE WHEN t.trx_type=0 and t.payment_method!=1 THEN true " + 
						"WHEN t.trx_type!=0 THEN true " + 
						"ELSE false END) and (t.insert_time between ? and ?) order by t.insert_time desc limit 100";
			
				try {
					weTopUpDS.prepareStatement(sql);
					weTopUpDS.getPreparedStatement().setString(1, userID);
					weTopUpDS.getPreparedStatement().setString(2, userID);
					weTopUpDS.getPreparedStatement().setString(3, userID);
					weTopUpDS.getPreparedStatement().setString(4, userID);
					weTopUpDS.getPreparedStatement().setString(5, userID);
					weTopUpDS.getPreparedStatement().setString(6, userPhone);
					weTopUpDS.getPreparedStatement().setString(7, userPhone);
					weTopUpDS.getPreparedStatement().setString(8, startDate);
					weTopUpDS.getPreparedStatement().setString(9, endDate);
					
					ResultSet rs = weTopUpDS.executeQuery();
					
					while (rs.next()) {
						//	user_id, trx_id, Ref. TrxID, amount, Purpose, Status Message, trx_type, trx_status, payment_method, Sender, Receiver, Date
						trx_history += "\"" + rs.getString(1) + "\"" + ",";
						trx_history += "\"" + rs.getString(2) + "\"" + ",";
						trx_history += "\"" + rs.getString(3) + "\"" + ",";
						trx_history += "\"" + rs.getString(4) + "\"" + ",";
						trx_history += "\"" + rs.getString(5) + "\"" + ",";
						trx_history += "\"" + rs.getString(6) + "\"" + ",";
						trx_history += "\"" + rs.getString(7) + "\"" + ",";
						trx_history += "\"" + rs.getString(8) + "\"" + ",";
						trx_history += "\"" + rs.getString(9) + "\"" + ",";
						trx_history += "\"" + rs.getString(10) + "\"" + ",";
						trx_history += "\"" + rs.getString(11) + "\"" + ",";
						trx_history += "\"" + rs.getString(12) + "\"" + "|";
					}
					int lio = trx_history.lastIndexOf("|");
					if (lio > 0)
						trx_history = trx_history.substring(0, lio);

					errorCode = "0";
					errorMessage = "fetched Stock transaction history successfully.";

					rs.close();
					weTopUpDS.closePreparedStatement();
					
				} catch (SQLException e) {
					e.printStackTrace();
					errorCode = "11";
					errorMessage = "SQL Exception.";
					LogWriter.LOGGER.severe(e.getMessage());
				}
			}
		}

		//LogWriter.LOGGER.info("trx_history :" + trx_history);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("trx_history", trx_history);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchTrxHistory(String userID, String userType, String phone) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String mode = "";

		String trx_history = "";
		String sql = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(userType)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			if (userType.equals("5")) {
				mode = userID;
				String userPhone = getUserPhone(userID);
//				sql = "SELECT trx_id, amount, trx_status, LP_trx_status, trx_type, date_format(insert_time, '%Y-%m-%d %H:%i:%S'), receiver_phone, payment_method, ref_trx_id  FROM transaction_log where trx_status!='6' and user_id=? and (trx_type!=0 and payment_method!=1) order by insert_time desc limit 100";
				
				sql = "SELECT trx_id, (amount+commission_amount) as amount, trx_status, LP_trx_status, trx_type, \n" + 
						"date_format(insert_time, '%Y-%m-%d %H:%i:%S'), user_id, payment_method, ref_trx_id  \n" + 
						"FROM transaction_log where trx_status not in (0,6) and (user_id=? or (trx_type=2 and receiver_phone="+ msisdnNormalize(userPhone)+")) and \n" + 
						"( CASE WHEN trx_type=0 and payment_method!=1 THEN true\n" + 
						"WHEN trx_type!=0 THEN true\n" +
						"ELSE false END)  order by insert_time desc limit 100";
			} else {
				mode = userID;
//				sql = "SELECT trx_id, amount, trx_status, LP_trx_status, trx_type, date_format(insert_time, '%Y-%m-%d %H:%i:%S'), receiver_phone, payment_method, ref_trx_id  FROM transaction_log where trx_status!='6' and user_id=? and (trx_type!=0 and payment_method!=1) order by insert_time desc limit 100";
				
				sql = "SELECT trx_id, amount, trx_status, LP_trx_status, trx_type, \n" + 
						"date_format(insert_time, '%Y-%m-%d %H:%i:%S'), receiver_phone, payment_method, ref_trx_id  \n" + 
						"FROM transaction_log where trx_status not in (0,6) and user_id=? and \n" + 
						"( CASE WHEN trx_type=0 and payment_method!=1 THEN true\n" + 
						"WHEN trx_type!=0 THEN true\n" + 
						"ELSE false END)  order by insert_time desc limit 100";
			}
			try {
				weTopUpDS.prepareStatement(sql);
				if (userType.equals("5")) {
					weTopUpDS.getPreparedStatement().setString(1, mode);
				}else {
					weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(mode));
				}
				
				ResultSet rs = weTopUpDS.executeQuery();
				
				while (rs.next()) {
					// trx_id, amount, trx_status, LP_trx_status, trx_type, insert_time, user_id
					trx_history += "\"" + rs.getString(1) + "\"" + ",";
					trx_history += "\"" + rs.getString(2) + "\"" + ",";
					trx_history += "\"" + rs.getString(3) + "\"" + ",";
					trx_history += "\"" + rs.getString(4) + "\"" + ",";
					trx_history += "\"" + rs.getString(5) + "\"" + ",";
					trx_history += "\"" + rs.getString(6) + "\"" + ",";
//					if (userType.equals("5")) {
//						trx_history += "\"" + getUserNameByID(rs.getString(7)) + "\"" + ",";
//					} else {
//						trx_history += "\"" + getUserNameByPhone(rs.getString(7)) + "\"" + ",";
//					}
					trx_history += "\"" + rs.getString(7) + "\"" + ",";
					
					trx_history += "\"" + rs.getString(8) + "\"" + ",";
					trx_history += "\"" + rs.getString(9) + "\"" + "|";
				}
				int lio = trx_history.lastIndexOf("|");
				if (lio > 0)
					trx_history = trx_history.substring(0, lio);

				errorCode = "0";
				errorMessage = "fetched transaction history successfully.";

				rs.close();
				weTopUpDS.closePreparedStatement();
				
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}

		//LogWriter.LOGGER.info("trx_history :" + trx_history);
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

		if (NullPointerExceptionHandler.isNullOrEmpty(userID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		}else {
			String sql = "select\n" + "-- JSON_LENGTH(JSON_ARRAYAGG(t.top_up_status)) counter,\n"
					+ "-- GROUP_CONCAT(t.top_up_status ORDER BY t.top_up_status DESC SEPARATOR ',') gc,\n"
					+ "-- JSON_OBJECTAGG(t.top_up_status, t.retry_counter) jsn,\n"
					+ "-- JSON_ARRAYAGG(t.top_up_status) ary,\n" + "-- max(t.retry_counter) rc,\n"
					+ "-- max(t.top_up_status),\n" + "-- ,max(rp.max_attempt)\n"
					+ "--  ,min(t.user_id),max(t.user_id),min(rp.id),max(rp.id)\n"
					+ "t.trx_id,min(t.payee_phone),min(tx.amount),min(tx.trx_status),min(lp_trx_status),\n"
					+ "case when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) then '4' -- definite success\n"
					+	"	 when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('0')) then '11' -- processing\n"
					+ "	  when !JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) \n"
					+ "           and  (max(t.retry_counter)>=any_value(rp.max_attempt) \n"
					+ "                  or max(tx.trx_status) !=2 ) then '10' -- definite fail\n"
					+ "	  else '11' -- processing\n" + " end topup_status,\n"
					+ "date_format(min(t.insert_time), '%Y-%m-%d %H:%i:%S'),min(tx.payment_method)\n" + "\n"
					+ "from topup_log t inner join transaction_log tx on t.trx_id=tx.trx_id \n"
					+ "left join retry_profile rp on tx.retry_profile = rp.id\n" + "where tx.user_id=? -- CHANGE\n"
					+ " and tx.trx_status not in (0,6) group by t.trx_id\n" + "order by min(t.insert_time) desc limit 100";
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
					// trx_id,payee_phone,amount,trx_status,LP_trx_status,top_up_status,insert_time,payment_method
					trx_history += "\"" + rs.getString(1) + "\"" + ",";
					trx_history += "\"" + rs.getString(2) + "\"" + ",";
					trx_history += "\"" + rs.getString(3) + "\"" + ",";
					trx_history += "\"" + rs.getString(4) + "\"" + ",";
					trx_history += "\"" + rs.getString(5) + "\"" + ",";
					trx_history += "\"" + rs.getString(6) + "\"" + ",";
					trx_history += "\"" + rs.getString(7) + "\"" + ",";
					trx_history += "\"" + rs.getString(8) + "\"" + "|";
				}
				int lio = trx_history.lastIndexOf("|");
				if (lio > 0)
					trx_history = trx_history.substring(0, lio);

				errorCode = "0";
				errorMessage = "fetched topup history successfully.";

				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}

		//	LogWriter.LOGGER.info("trx_history :" + trx_history);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("trx_history", trx_history);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchShadowOpBalanceHistory(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String trx_history = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(userID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		}else {
			if(checkIfManagerialAdmin(userID)) {
				String sql = "select\n" + "-- JSON_LENGTH(JSON_ARRAYAGG(t.top_up_status)) counter,\n"
						+ "-- GROUP_CONCAT(t.top_up_status ORDER BY t.top_up_status DESC SEPARATOR ',') gc,\n"
						+ "-- JSON_OBJECTAGG(t.top_up_status, t.retry_counter) jsn,\n"
						+ "-- JSON_ARRAYAGG(t.top_up_status) ary,\n" + "-- max(t.retry_counter) rc,\n"
						+ "-- max(t.top_up_status),\n" + "-- ,max(rp.max_attempt)\n"
						+ "--  ,min(t.user_id),max(t.user_id),min(rp.id),max(rp.id)\n"
						+ "t.trx_id,min(t.payee_phone),min(tx.amount),min(tx.trx_status),min(lp_trx_status),\n"
						+ "case when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) then '4' -- definite success\n"
						+	"	 when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('0')) then '11' -- processing\n"
						+ "	  when !JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) \n"
						+ "           and  (max(t.retry_counter)>=any_value(rp.max_attempt) \n"
						+ "                  or max(tx.trx_status) !=2 ) then '10' -- definite fail\n"
						+ "	  else '11' -- processing\n" + " end topup_status,\n"
						+ "date_format(min(t.insert_time), '%Y-%m-%d %H:%i:%S'),min(tx.payment_method)\n" + "\n"
						+ "from topup_log t inner join transaction_log tx on t.trx_id=tx.trx_id \n"
						+ "left join retry_profile rp on tx.retry_profile = rp.id\n" + "where tx.user_id=? -- CHANGE\n"
						+ " and tx.trx_status not in (0,6) group by t.trx_id\n" + "order by min(t.insert_time) desc limit 100";
				try {
					weTopUpDS.prepareStatement(sql);
					weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
					ResultSet rs = weTopUpDS.executeQuery();
					while (rs.next()) {
						// trx_id,payee_phone,amount,trx_status,LP_trx_status,top_up_status,insert_time,payment_method
						trx_history += "\"" + rs.getString(1) + "\"" + ",";
						trx_history += "\"" + rs.getString(2) + "\"" + ",";
						trx_history += "\"" + rs.getString(3) + "\"" + ",";
						trx_history += "\"" + rs.getString(4) + "\"" + ",";
						trx_history += "\"" + rs.getString(5) + "\"" + ",";
						trx_history += "\"" + rs.getString(6) + "\"" + ",";
						trx_history += "\"" + rs.getString(7) + "\"" + ",";
						trx_history += "\"" + rs.getString(8) + "\"" + "|";
					}
					int lio = trx_history.lastIndexOf("|");
					if (lio > 0)
						trx_history = trx_history.substring(0, lio);

					errorCode = "0";
					errorMessage = "fetched topup history successfully.";

					rs.close();
					weTopUpDS.closePreparedStatement();
				} catch (SQLException e) {
					e.printStackTrace();
					errorCode = "11";
					errorMessage = "SQL Exception.";
					LogWriter.LOGGER.severe(e.getMessage());
				}
			} else {
				errorCode = "-5";
				errorMessage = "User not Authorized.";
			}
			
		}

		//	LogWriter.LOGGER.info("trx_history :" + trx_history);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("trx_history", trx_history);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchFileTopUpHistory(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String trx_history = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(userID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		}else {
			String sql = "select -- JSON_LENGTH(JSON_ARRAYAGG(t.top_up_status)) counter,\\n\"\n" + 
					"					-- GROUP_CONCAT(t.top_up_status ORDER BY t.top_up_status DESC SEPARATOR ',') gc,\\n\"\n" + 
					"					-- JSON_OBJECTAGG(t.top_up_status, t.retry_counter) jsn,\\n\"\n" + 
					"					-- JSON_ARRAYAGG(t.top_up_status) ary,\\n\" + \"-- max(t.retry_counter) rc,\\n\"\n" + 
					"					-- max(t.top_up_status),\\n\" + \"-- ,max(rp.max_attempt)\\n\"\n" + 
					"					--  ,min(t.user_id),max(t.user_id),min(rp.id),max(rp.id)\\n\"\n" + 
					"					t.trx_id,min(t.payee_phone),min(tx.amount),min(tx.trx_status),min(lp_trx_status),\n" + 
					"					case when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) then '4' -- definite success\n" +
					"	 when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('0')) then '11' -- processing\n" +
					"						  when !JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) \n" + 
					"					           and  (max(t.retry_counter)>=any_value(rp.max_attempt)) then '10' -- definite fail\n" + 
					"						  else '11' -- processing\n" + 
					"                          end topup_status,\n" + 
					"					date_format(min(t.insert_time), '%Y-%m-%d %H:%i:%S'),min(tx.payment_method),tf.file_name,tf.updated_file_name\n" + 
					"					from topup_log t inner join transaction_log tx on t.trx_id=tx.trx_id \n" + 
					"					left join retry_profile rp on tx.retry_profile = rp.id left join topup_file_info tf on tx.ref_file_id=tf.id where tx.user_id=? -- CHANGE\\n\"\n" + 
					"					and tx.trx_status in (6) group by t.trx_id order by min(t.insert_time) desc limit 500";
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
					// trx_id,payee_phone,amount,trx_status,LP_trx_status,top_up_status,insert_time,payment_method
					trx_history += "\"" + rs.getString(1) + "\"" + ",";
					trx_history += "\"" + rs.getString(2) + "\"" + ",";
					trx_history += "\"" + rs.getString(3) + "\"" + ",";
					trx_history += "\"" + rs.getString(4) + "\"" + ",";
					trx_history += "\"" + rs.getString(5) + "\"" + ",";
					trx_history += "\"" + rs.getString(6) + "\"" + ",";
					trx_history += "\"" + rs.getString(7) + "\"" + ",";
					trx_history += "\"" + rs.getString(8) + "\"" + ",";
					trx_history += "\"" + rs.getString(9) + "\"" + ",";
					trx_history += "\"" + rs.getString(10) + "\"" + "|";
				}
				int lio = trx_history.lastIndexOf("|");
				if (lio > 0)
					trx_history = trx_history.substring(0, lio);

				errorCode = "0";
				errorMessage = "fetched topup history successfully.";

				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}

		//	LogWriter.LOGGER.info("trx_history :" + trx_history);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("trx_history", trx_history);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchSingleFileTopUpHistory(String userID,String fileID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String trx_history = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(fileID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		}else {
			String sql = "select -- JSON_LENGTH(JSON_ARRAYAGG(t.top_up_status)) counter,\\n\"\n" + 
					"					-- GROUP_CONCAT(t.top_up_status ORDER BY t.top_up_status DESC SEPARATOR ',') gc,\\n\"\n" + 
					"					-- JSON_OBJECTAGG(t.top_up_status, t.retry_counter) jsn,\\n\"\n" + 
					"					-- JSON_ARRAYAGG(t.top_up_status) ary,\\n\" + \"-- max(t.retry_counter) rc,\\n\"\n" + 
					"					-- max(t.top_up_status),\\n\" + \"-- ,max(rp.max_attempt)\\n\"\n" + 
					"					--  ,min(t.user_id),max(t.user_id),min(rp.id),max(rp.id)\\n\"\n" + 
					"					t.trx_id,min(t.payee_phone),min(tx.amount),min(tx.trx_status),min(lp_trx_status),\n" + 
					"					case when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) then '4' -- definite success\n" + 
					"	 when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('0')) then '11' -- processing\n" +
					"						  when !JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) \n" + 
					"					           and  (max(t.retry_counter)>=any_value(rp.max_attempt)) then '10' -- definite fail\n" + 
					"						  else '11' -- processing\n" + 
					"                          end topup_status,\n" + 
					"					date_format(min(t.insert_time), '%Y-%m-%d %H:%i:%S'),min(tx.payment_method),tf.file_name,tf.updated_file_name\n" + 
					"					from topup_log t inner join transaction_log tx on t.trx_id=tx.trx_id \n" + 
					"					left join retry_profile rp on tx.retry_profile = rp.id left join topup_file_info tf on tx.ref_file_id=tf.id where tx.user_id=? -- CHANGE\\n\"\n" + 
					"					and tx.trx_status in (6) and tx.ref_file_id = ? group by t.trx_id order by min(t.insert_time) desc limit 500";
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(userID));
				weTopUpDS.getPreparedStatement().setInt(2, Integer.parseInt(fileID));
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
					// trx_id,payee_phone,amount,trx_status,LP_trx_status,top_up_status,insert_time,payment_method,file_name,updated_file_name
					trx_history += "\"" + rs.getString(1) + "\"" + ",";
					trx_history += "\"" + rs.getString(2) + "\"" + ",";
					trx_history += "\"" + rs.getString(3) + "\"" + ",";
					trx_history += "\"" + rs.getString(4) + "\"" + ",";
					trx_history += "\"" + rs.getString(5) + "\"" + ",";
					trx_history += "\"" + rs.getString(6) + "\"" + ",";
					trx_history += "\"" + rs.getString(7) + "\"" + ",";
					trx_history += "\"" + rs.getString(8) + "\"" + ",";
					trx_history += "\"" + rs.getString(9) + "\"" + ",";
					trx_history += "\"" + rs.getString(10) + "\"" + "|";
				}
				int lio = trx_history.lastIndexOf("|");
				if (lio > 0)
					trx_history = trx_history.substring(0, lio);

				errorCode = "0";
				errorMessage = "fetched topup history successfully.";

				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}

		//	LogWriter.LOGGER.info("trx_history :" + trx_history);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("trx_history", trx_history);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder fetchTopUpStatusApi(String userID, String trxID, String user_trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String trx_status = "";
		String topup_status = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || (NullPointerExceptionHandler.isNullOrEmpty(trxID)
				&& NullPointerExceptionHandler.isNullOrEmpty(user_trx_id))) {
			errorCode = "0009";
		} else {
			JsonDecoder jd = new JsonDecoder(getTopUpStatus(userID, trxID, user_trx_id).getJsonObject().toString());

			if (jd.getNString("ErrorCode").equals("0")) {
				trx_status = jd.getNString("trx_status");
				topup_status = jd.getNString("top_up_status");
				trxID = jd.getNString("trx_id");
				user_trx_id = jd.getNString("user_trx_id");

				jsonEncoder.addElement("trx_id", trxID);
				jsonEncoder.addElement("user_trx_id",
						(NullPointerExceptionHandler.isNullOrEmpty(user_trx_id) ? "" : user_trx_id));
				jsonEncoder.addElement("trx_status", trx_status);
				jsonEncoder.addElement("topup_status", topup_status);
				errorCode = "0000";
			} else {
				errorCode = "0021";
			}

		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder insertTopupApi(String userID, String phone, String amount, String op, String isPostpaid,
			String user_trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";

		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || (NullPointerExceptionHandler.isNullOrEmpty(phone)
				&& NullPointerExceptionHandler.isNullOrEmpty(amount))) {
			errorCode = "0009";
		} else {
			// insert to transaction_log
			String trx_id = "";
			trx_id = RandomStringGenerator.getRandomString("0123456789", 2);
			// topup_trx_id = topup_trx_id +
			// RandomStringGenerator.getRandomString("abcdefghijklmnopqrstuvwxyz", 4);
			trx_id = trx_id + RandomStringGenerator
					.getRandomString("0123456789abcdefghABCDEFGHIJKLMNOPQRSTUVWXYZijklmnopqrstuvwxyz", 7);
			trx_id = trx_id + RandomStringGenerator.getRandomString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 2);

			String trx_type = "0"; // for topup
			op = (NullPointerExceptionHandler.isNullOrEmpty(op) ? "" : op);
			isPostpaid = (NullPointerExceptionHandler.isNullOrEmpty(isPostpaid) ? "" : isPostpaid);

			String opType = isPostpaid.equals("1") ? "2" : "1";

			if (op.equals("AT") || op.equals("RO") || op.equals("BL") || op.equals("GP") || op.equals("TT")) {
				op = getOperator(op);
			} else if (op.equals("")) {
				op = getOperator(phone);
			} else {
				errorCode = "0026"; // invalid OP
				jsonEncoder.addElement("ErrorCode", errorCode);
				jsonEncoder.buildJsonObject();
				return jsonEncoder;
			}
//			String opGrants = fetchUserApiOpGrants(userID);
//
//			if (opGrants.charAt(Integer.parseInt(op)) == '1') {
//				LogWriter.LOGGER.info("operator not permitted.");
//				errorCode = "0027"; // OP not permitted
//				jsonEncoder.addElement("ErrorCode", errorCode);
//				jsonEncoder.buildJsonObject();
//				return jsonEncoder;
//			}

			LogWriter.LOGGER.info("userID :" + userID 
					+ "\namount :" + amount 
					+ "\ntrx_id :" + trx_id 
					+ "\nuser_trx_id :" + (NullPointerExceptionHandler.isNullOrEmpty(user_trx_id) ? "" : user_trx_id)
					+ "\ntrx_type :" + trx_type
					+ "\nop :" + op
					+ "\nopType :" + opType
					+ "\nphone :" + phone);

			

			errorCode = insertTransaction(userID, amount, trx_id, user_trx_id, "API", trx_type, op, opType, null, phone,
					"", "", "N", null, "").getJsonObject().toString();

			//LogWriter.LOGGER.info("errorCode :" + errorCode);

			JsonDecoder jd = new JsonDecoder(errorCode);
			String trxErrorCode = "";
			String topUpErrorCode = "";
			String opBalanceFlag = "";
			String userBalanceFlag = "";

			trxErrorCode = jd.getNString("trxErrorCode");
			topUpErrorCode = jd.getNString("topUpErrorCode");
			opBalanceFlag = jd.getNString("opBalanceFlag");
			userBalanceFlag = jd.getNString("userBalanceFlag");

			LogWriter.LOGGER.info("trxErrorCode :" + trxErrorCode);
			LogWriter.LOGGER.info("topUpErrorCode :" + topUpErrorCode);
			LogWriter.LOGGER.info("opBalanceFlag :" + opBalanceFlag);
			LogWriter.LOGGER.info("userBalanceFlag :" + userBalanceFlag);

			if (userBalanceFlag.equals("5")) {
				errorCode = "0013";
			} else if (opBalanceFlag.equals("5")) {
				errorCode = "0024";
			} else if (trxErrorCode.equals("0") && topUpErrorCode.equals("0")) {
				errorCode = "0000";
				jsonEncoder.addElement("trx_id", trx_id);
				jsonEncoder.addElement("user_trx_id",
						(NullPointerExceptionHandler.isNullOrEmpty(user_trx_id) ? "" : user_trx_id));
			} else if (trxErrorCode.equals("0")) {
				errorCode = "0022";
			} else if (trxErrorCode.equals("30")) {
				errorCode = "0027";	// OP not permitted
			}

		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder fetchUploadedFileDetails(String fileID,String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";
		String file_details = "";
		String total_msisdn = "0";
		String success_msisdn = "0";
		String total_amount = "0";

		if (NullPointerExceptionHandler.isNullOrEmpty(fileID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {

			JsonDecoder jd =  new JsonDecoder(getFileCountSummary(fileID,userID).getJsonObject().toString());
			total_msisdn = jd.getNString("totalCount");
			total_amount = jd.getNString("amount");
			success_msisdn = jd.getNString("validCount");

			String sql = "select phone, amount, operator, isPostpaid, insert_date, flag, file_row_id from file_topup_queue where file_id=? order by file_row_id asc limit 1000";
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, fileID);
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
					file_details += "\"" + rs.getString(1) + "\"" + ",";
					file_details += "\"" + rs.getString(2) + "\"" + ",";
					file_details += "\"" + rs.getString(3) + "\"" + ",";
					file_details += "\"" + rs.getString(4) + "\"" + ",";
					file_details += "\"" + rs.getString(5) + "\"" + ",";
					file_details += "\"" + rs.getString(6) + "\"" + ",";
					file_details += "\"" + rs.getString(7) + "\"" + "|";
				}
				int lio = file_details.lastIndexOf("|");
				if (lio > 0)
					file_details = file_details.substring(0, lio);

				errorCode = "0";
				errorMessage = "fetchUploadedFileDetails : successful";

				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("TotalMsisdn", total_msisdn);
		jsonEncoder.addElement("Success_msisdn", success_msisdn);
		jsonEncoder.addElement("TotalAmount", total_amount);
		jsonEncoder.addElement("fileID", fileID);
		jsonEncoder.addElement("FileDetails", file_details);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder downloadUploadedFileDetails(String fileID, String userID, String outputType, String reportType) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";
		String downloadID = "";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(fileID) || NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(outputType)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sql = "";
			
			if(reportType.equals("0")) {
				sql = "select phone as Phone, amount as Amount, operator as Operator, isPostpaid, insert_date as Insert_time  from file_topup_queue where file_id="+fileID+" order by file_row_id asc";
			}else {
				sql = "select t.trx_id as TransactionID,min(t.payee_phone) as Phone,min(tx.amount) as Amount,\n" + 
						"case when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) then 'Success' -- definite success\n" +
						"	 when JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('0')) then '11' -- processing\n" +
						"	  when !JSON_CONTAINS(JSON_ARRAYAGG(t.top_up_status),JSON_ARRAY('4')) \n" + 
						"           and  (max(t.retry_counter)>=any_value(rp.max_attempt) \n" + 
						"                  or max(tx.trx_status) !=6 ) then 'Failed' -- definite fail\n" + 
						"	  else 'Processing' -- processing \n" + 
						"      end Topup_status,\n" + 
						"date_format(min(t.insert_time), '%Y-%m-%d %H:%i:%S') as Time\n" + 
						"from topup_log t inner join transaction_log tx on t.trx_id=tx.trx_id \n" + 
						"left join retry_profile rp on tx.retry_profile = rp.id where tx.user_id="+userID+" and tx.ref_file_id="+fileID+"-- CHANGE\n" + 
						"group by t.trx_id  order by min(t.insert_time) desc";
			}
			
			String sqlQuery = "INSERT INTO file_dump_query (`user_id`,`query`,`output_type`) VALUES (?,?,?)";
			try {
				weTopUpDS.prepareStatement(sqlQuery, true);
				weTopUpDS.getPreparedStatement().setString(1, userID);
				weTopUpDS.getPreparedStatement().setString(2, sql);
				weTopUpDS.getPreparedStatement().setString(3, outputType);
				
				errorCode = "0";
				errorMessage = "Successfully Inserted";
				weTopUpDS.execute();
				downloadID = getNewInsertID();
				LogWriter.LOGGER.info("downloadID : "+downloadID);
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("downloadID", downloadID);
		
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public JsonEncoder fetchInvalidFileRow(String fileID,String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";
		String invalidRows = "";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(fileID)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		} else {
			String sql = "select file_row_id, phone, amount, operator, isPostpaid from file_topup_queue where file_id=? and user_id=? "
					+ "and ((isPostpaid is not null and isPostpaid not in ('','0','1')) "
					+ "or( operator is not null and operator not in ('GP','BL','RO','AT','TT','')) "
					+ "or (amount not between 10 and 5000) "
					+ "or( phone not REGEXP '^[[:<:]]((880)|(0))?(1[3-9]{1}|35|44|66){1}[[:digit:]]{8}[[:>:]]'))  "
					+ "order by file_row_id asc limit 1000";
			
						
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(fileID));
				weTopUpDS.getPreparedStatement().setInt(2, Integer.parseInt(userID));
				ResultSet rs = weTopUpDS.executeQuery();
				while (rs.next()) {
					invalidRows += "\"" + rs.getString(1) + "\"" + ",";
					invalidRows += "\"" + rs.getString(2) + "\"" + ",";
					invalidRows += "\"" + rs.getString(3) + "\"" + ",";
					invalidRows += "\"" + rs.getString(4) + "\"" + ",";
					invalidRows += "\"" + rs.getString(5) + "\"" + "|";
				}
				int lio = invalidRows.lastIndexOf("|");
				if (lio > 0)
					invalidRows = invalidRows.substring(0, lio);

				errorCode = "0";
				errorMessage = "fetchUploadedFileDetails : successful";

				rs.close();
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				errorCode = "11";
				errorMessage = "SQL Exception.";
				LogWriter.LOGGER.severe(e.getMessage());
			}
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("fileID", fileID);
		jsonEncoder.addElement("InvalidRows", invalidRows);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder processFileTopups(String userID, String fileID, String processFlag) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		String trx_id = "";

		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(fileID) || NullPointerExceptionHandler.isNullOrEmpty(processFlag)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters for insertTransactionLog.";
		} else{
			//	check if already updated
			if(checkIfFileAlreadyProcessed(fileID,userID)) {
				errorCode = "10";
				errorMessage = "Already processed.";
			}else {
				if(processFlag.equals("0")) {
					// proceed file topup
					JsonDecoder jd =  new JsonDecoder(getFileCountSummary(fileID,userID).getJsonObject().toString());
					int total_amount = Integer.parseInt(jd.getNString("amount"));
					
					if(total_amount>0) {
						Double userBalance = getUserBalance(userID);
						LogWriter.LOGGER.info("userBalance : " + userBalance);

						if (userBalance < total_amount) {
							errorCode = "16";
							errorMessage = "Low user balance.";
						} else {
							
							//	TO-DO INSERT TRANSACTION
							trx_id = RandomStringGenerator.getRandomString("0123456789", 2);
							// topup_trx_id = topup_trx_id +
							// RandomStringGenerator.getRandomString("abcdefghijklmnopqrstuvwxyz", 4);
							trx_id = trx_id + RandomStringGenerator
									.getRandomString("0123456789abcdefghABCDEFGHIJKLMNOPQRSTUVWXYZijklmnopqrstuvwxyz", 7);
							trx_id = trx_id + RandomStringGenerator.getRandomString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 2);
							String trx_type = "4";
							String trxErrorCode = "";
							String topUpErrorCode = "";
							String userBalanceFlag = "";
							String deductTrx_id = "";
							try {
								errorCode = insertTransaction(userID, ""+total_amount, trx_id, trx_id, "wetopup", trx_type, null, null, null, null,
										"", "", "N", fileID, "").getJsonObject().toString();

								//LogWriter.LOGGER.info("InsertTRX_response :" + errorCode);

								JsonDecoder jd2 = new JsonDecoder(errorCode);
								
								trxErrorCode = jd2.getNString("trxErrorCode");
								topUpErrorCode = jd2.getNString("topUpErrorCode");
								userBalanceFlag = jd2.getNString("userBalanceFlag");
								deductTrx_id = jd2.getNString("trx_id");

								LogWriter.LOGGER.info("trxErrorCode :" + trxErrorCode);
								LogWriter.LOGGER.info("topUpErrorCode :" + topUpErrorCode);
								LogWriter.LOGGER.info("userBalanceFlag :" + userBalanceFlag);
								LogWriter.LOGGER.info("deductTrx_id :" + deductTrx_id);
							}catch(Exception e) {
								e.printStackTrace();
							}
							
//							//	deduct balance
//							boolean deductFlag = deductUserBalance(userID, total_amount*1.0);
//							LogWriter.LOGGER.info("balance deduction : " + deductFlag);
							
							if(topUpErrorCode.equals("0")) {
								//	call file_topup_Q_processor
								userBalance = getUserBalance(userID);
								LogWriter.LOGGER.info("deducted userBalance : " + userBalance);
								
								String sql = "{call file_topup_Q_processor(?,?,?,?,?)}";
								
								String trxPrefix = RandomStringGenerator.getRandomString("0123456789", 2);
								trxPrefix = trxPrefix + RandomStringGenerator.getRandomString("abcdefghijklmnopqrstuvwxyz", 4);
								trxPrefix = trxPrefix + RandomStringGenerator.getRandomString("0123456789abcdefghABCDEFGHIJKLMNOPQRSTUVWXYZijklmnopqrstuvwxyz", 3);
								trxPrefix = trxPrefix + RandomStringGenerator.getRandomString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 2);
								
								try {
									weTopUpDS.prepareCall(sql);
									weTopUpDS.getCallableStatement().registerOutParameter(1, Types.INTEGER);
									weTopUpDS.getCallableStatement().setInt(2, Integer.parseInt(userID));
									weTopUpDS.getCallableStatement().setInt(3, Integer.parseInt(fileID));
									weTopUpDS.getCallableStatement().setString(4, trxPrefix);
									weTopUpDS.getCallableStatement().setString(5, deductTrx_id);
									
									
									weTopUpDS.getCallableStatement().execute();
									
									int procResponse = weTopUpDS.getCallableStatement().getInt(1);
								
									LogWriter.LOGGER.info("procResponse : "+procResponse);
									
									weTopUpDS.closeCallableStatement();
								
									if(procResponse == 1) {
										if(updateTopupFileStatus(fileID, userID, 3)) {
											errorCode = "0";
											errorMessage = "process action successful.";
										}else {
											errorCode = "-10";
											errorMessage = "updateTopupFileStatus failed.";
										}
									} else {
										errorCode = "-15";
										errorMessage = "topUpQueueProc run failed.";
									    userBalance = getUserBalance(userID);
										//LogWriter.LOGGER.info("before -1 proc userBalance : " + userBalance);
										Boolean flag = rechargeUserBalanceByID(userID, total_amount*1.0);
										if (flag) {
											errorCode = "13";
											errorMessage = "Balance credit successful";
											String additional_info = errorMessage;
											JsonDecoder json = new JsonDecoder(updateTransactionStatus(trx_id, "4", additional_info+" : "+total_amount, trx_type).getJsonObject().toString());
											LogWriter.LOGGER.info("updateErrorCode : "+json.getNString("ErrorCode"));
											LogWriter.LOGGER.info("updateErrorMessage : "+json.getNString("ErrorMessage"));
										}
										else {
											errorCode = "14";
											errorMessage = "Balance credit failed.";
										}
										userBalance = getUserBalance(userID);
										LogWriter.LOGGER.info("after -1 proc userBalance : " + userBalance);
									}
									
									
									
								} catch (Exception e) {
									LogWriter.LOGGER.severe(e.getMessage());
									e.printStackTrace();
									userBalance = getUserBalance(userID);
									LogWriter.LOGGER.info("exception userBalance : " + userBalance);
									Boolean flag = rechargeUserBalanceByID(userID, total_amount*1.0);
									if (flag) {
										errorCode = "13";
										errorMessage = "Balance credit successful";
										String additional_info = errorMessage;
										JsonDecoder json = new JsonDecoder(updateTransactionStatus(trx_id, "4", additional_info+" : "+total_amount, trx_type).getJsonObject().toString());
										LogWriter.LOGGER.info("updateErrorCode : "+json.getNString("ErrorCode"));
										LogWriter.LOGGER.info("updateErrorMessage : "+json.getNString("ErrorMessage"));
									}
									else {
										errorCode = "14";
										errorMessage = "Balance credit failed.";
									}
									userBalance = getUserBalance(userID);
									LogWriter.LOGGER.info("exception userBalance : " + userBalance);
								}
							} else {
								errorCode = "15";
								errorMessage = "User balance deduction failed.";
							}
						}
					}
				}else {
					//	cancel file topup
					if(updateTopupFileStatus(fileID, userID, 5)) {
						errorCode = "0";
						errorMessage = "process action successful.";
					}else {
						errorCode = "-10";
						errorMessage = "updateTopupFileStatus failed.";
					}
				}
			}		
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public boolean updateTopupFileStatus(String fileID, String userID, int status) {
		boolean flag = false;

		String sql = "update topup_file_info set status=? where id=? and user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, status);
			weTopUpDS.getPreparedStatement().setInt(2, Integer.parseInt(fileID));
			weTopUpDS.getPreparedStatement().setInt(3, Integer.parseInt(userID));
			weTopUpDS.execute();
			flag = true;
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}

	public boolean checkIfFileAlreadyProcessed(String id, String userID) {
		boolean flag = false;
		String sql = "SELECT status FROM topup_file_info where id = ? and user_id = ?";
		int status = -1;
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(id));
			weTopUpDS.getPreparedStatement().setInt(2, Integer.parseInt(userID));
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				status = rs.getInt(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		if (status == 2) {
			//LogWriter.LOGGER.info("File not updated yet.");
		} else {
			flag = true;
			//LogWriter.LOGGER.info("File already updated.");
		}
		return flag;
	}

	public boolean checkIfManagerialAdmin(String id) {
		boolean flag = false;
		String sql = "SELECT count(*) FROM `admins_info` WHERE user_id=?";
		int count = 0;
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				count = rs.getInt(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		if (count > 0) {
			flag = true;
			//LogWriter.LOGGER.info("user verified as Admin.");
		} else {
			//LogWriter.LOGGER.info("user is not authorized to perform this action.");
		}
		return flag;
	}
	
	public boolean checkIfAdmin(String id) {
		boolean flag = false;
		String sql = "SELECT count(*) FROM `users_info` WHERE user_id=? and user_type =?";
		int count = 0;
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			weTopUpDS.getPreparedStatement().setInt(2, 3); // for admin
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				count = rs.getInt(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		if (count > 0) {
			flag = true;
			//LogWriter.LOGGER.info("user verified as Admin.");
		} else {
			//LogWriter.LOGGER.info("user is not authorized to perform this action.");
		}
		return flag;
	}

	public JsonEncoder getFileCountSummary(String fileID,String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		
//		manual OP check
//		String opGrants = fetchUserApiOpGrants(userID);
//
//		+ "and (t.operator is null or t.operator in ("
//		+ (opGrants.charAt(0)=='0'?"'AT',":"")
//		+ (opGrants.charAt(1)=='0'?"'RO',":"")
//		+ (opGrants.charAt(2)=='0'?"'GP',":"")
//		+ (opGrants.charAt(3)=='0'?"'BL',":"")
//		+ (opGrants.charAt(4)=='0'?"'TT',":"")
//		+ "'')) "
		
		String sql = "select count(t.phone) as total_msisdn, sum(t.amount) as total_amount, "
				+ "i.file_name, i.updated_file_name, i.insert_time from file_topup_queue t inner join topup_file_info i on t.file_id=i.id "
				+ "where t.file_id=? and (t.isPostpaid is null or t.isPostpaid in ('','0','1')) "
				+ "and (t.operator is null or t.operator in ('AT','RO','GP','BL','TT','')) "
				+ "and (t.amount between 10 and 5000) "
				+ "and (t.phone REGEXP '^[[:<:]]((880)|(0))?(1[3-9]{1}|35|44|66){1}[[:digit:]]{8}[[:>:]]') "
				+ "group by t.file_id";
		String errorCode = "-1";
		String errorMessage = "Not initiated.";
		String validCount = "0";				
		String amount = "0";
		String fileName = "";
		String updatedFileName = "";
		String insertTime = "";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setInt(1, Integer.parseInt(fileID));
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				validCount = rs.getString(1);				
				amount = rs.getString(2);
				fileName = rs.getString(3);
				updatedFileName = rs.getString(4);
				insertTime = rs.getString(5);
				LogWriter.LOGGER.info("total_msisdn : " + rs.getString(1) + "\ntotal_amount : " + rs.getString(2)  + "\nfileName : " + rs.getString(3)  + "\nupdatedFileName : " + rs.getString(4) + "\ninsert_time : " + rs.getString(5));	
			}
			errorCode = "0";
			errorMessage = "fecthed successfully.";
			jsonEncoder.addElement("validCount", validCount);
			jsonEncoder.addElement("totalCount", ""+getFileEntryCount(fileID));
			jsonEncoder.addElement("amount",amount);
			jsonEncoder.addElement("fileName", fileName);
			jsonEncoder.addElement("updatedFileName", updatedFileName);
			jsonEncoder.addElement("insertTime", insertTime);
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQLException";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	public int getFileEntryCount(String id) {
		int count = 0;
		String sql = "select count(*) from file_topup_queue where file_id=? group by file_id";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				count = rs.getInt(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return count;
	}

	public double getShadowBalance(String id) {
		// id = 1 = airtel
		// id = 2 = robi
		// id = 3 = paywell
		// id = 4 = banglalink
		// id = 5 = grameenphone
		// id = 6 = payStation
		// id = 7 = skitto
		double balance = 0.0;
		String sql = "SELECT operator_balance FROM `shadow_balance_info` WHERE id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				balance = rs.getDouble(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return balance;
	}
	
	public boolean deductShadowBalance(String id, int amount) {
		boolean flag = false;
		String sql = "UPDATE shadow_balance_info SET operator_balance = (operator_balance - ?) WHERE id = ?";

		try {
			weTopUpDS.prepareStatement(sql,true);
			weTopUpDS.getPreparedStatement().setInt(1, amount);
			weTopUpDS.getPreparedStatement().setString(2, id);
			
			long count = weTopUpDS.executeUpdate();
			weTopUpDS.closePreparedStatement();
			if(count>0) {
				flag = true;
			}
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}
	
	public boolean rechargeShadowBalance(String id, int amount) {
		boolean flag = false;
		String sql = "UPDATE shadow_balance_info SET operator_balance = (operator_balance + ?) WHERE id = ?";

		try {
			weTopUpDS.prepareStatement(sql,true);
			weTopUpDS.getPreparedStatement().setInt(1, amount);
			weTopUpDS.getPreparedStatement().setString(2, id);
			
			long count = weTopUpDS.executeUpdate();
			weTopUpDS.closePreparedStatement();
			if(count>0) {
				flag = true;
			}
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}
	
	public double getOpBalance(String id) {
		// id = 1 = airtel
		// id = 2 = robi
		// id = 3 = paywell
		// id = 4 = banglalink
		// id = 5 = grameenphone
		double balance = 0.0;
		String sql = "SELECT operator_balance FROM `balance_info` WHERE id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				balance = rs.getDouble(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return balance;
	}
	
	public String getOpConfig(String id) {
		//	input :		0=Airtel ; 1=Robi ; 2=GrameenPhone ; 3=Banglalink ; 4=Teletalk; 5 = skitto
		
		//	output :	1 = airtel; 2 = robi; 3 = paywell; 4 = banglalink; 5 = grameenphone;	7 = skitto
		String opFlag = "";
		String sql = "SELECT topup_gateway FROM operator_configuration where id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				opFlag = rs.getString(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return opFlag;
	}

	public String getUserNameByID(String id) {
		String userName = "";
		String sql = "SELECT user_name FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				userName = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return userName;
	}

	public String getUserID(String id) {
		String userID = "";
		String sql = "SELECT user_id FROM users_info where phone=? or user_email=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, msisdnNormalize(id));
			weTopUpDS.getPreparedStatement().setString(2, id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				userID = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return userID;
	}
	
	public String getUserNameByPhone(String phone) {
		String userName = "";
		String sql = "SELECT user_name FROM users_info where phone=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, phone);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				userName = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return userName;
	}

	public String fetchUserStockConfigurations(String userID) {
		String stock_configuration = "00000";
		String sql = "SELECT stock_configuration FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				stock_configuration = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return stock_configuration;
	}
	
	public String fetchUserApiOpGrants(String userID) {
		String api_op_grants = "";
		String sql = "SELECT api_op_grants FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				api_op_grants = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return api_op_grants;
	}

	public String getUserEmail(String userID) {
		String userEmail = "";
		String sql = "SELECT user_email FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				userEmail = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return NullPointerExceptionHandler.isNullOrEmpty(userEmail)?"":userEmail;
	}
	
	public String getUserName(String userID) {
		String user_name = "";
		String sql = "SELECT user_name FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				user_name = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		return NullPointerExceptionHandler.isNullOrEmpty(user_name)?"":user_name;
	}

	public String getUserPhone(String userID) {
		String userPhone = "";
		String sql = "SELECT phone FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				userPhone = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return NullPointerExceptionHandler.isNullOrEmpty(userPhone)?"":userPhone;
	}

	public String getUserType(String userID) {
		String userType = "";
		
		String sql = "SELECT user_type FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				userType = rs.getString(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return userType;
	}
	
	public double getUserBalance(String user_id) {
		double balance = 0.0;
		String sql = "SELECT balance FROM users_info where user_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, user_id);
			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				balance = rs.getDouble(1);
			}
			rs.close();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return balance;
	}

	public boolean deductUserBalance(String user_id, Double amount) {
		boolean flag = false;
		String sql = "UPDATE users_info SET balance = round((balance - ?),6) WHERE user_id = ?";

		try {
			weTopUpDS.prepareStatement(sql,true);
			weTopUpDS.getPreparedStatement().setDouble(1, amount);
			weTopUpDS.getPreparedStatement().setString(2, user_id);
			long count = weTopUpDS.executeUpdate();
			weTopUpDS.closePreparedStatement();
			if(count>0) {
				flag = true;
			}
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}

	public boolean rechargeUserBalanceByID(String user_id, Double amount) {
		boolean flag = false;
		String sql = "UPDATE users_info SET balance = round((balance + ?),6) WHERE user_id = ?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setDouble(1, amount);
			weTopUpDS.getPreparedStatement().setString(2, user_id);
			weTopUpDS.execute();
			flag = true;
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}

	public boolean rechargeUserBalanceByPhoneOld(String payee_phone, Double amount) {
		boolean flag = false;
		// String sql = "UPDATE users_info SET balance = FORMAT((balance + ?),2) WHERE
		// phone=?";
		String sql = "UPDATE users_info SET balance =  round((balance + ?),6) WHERE phone=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setDouble(1, amount);
			weTopUpDS.getPreparedStatement().setString(2, msisdnNormalize(payee_phone));
			weTopUpDS.execute();
			flag = true;
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}
	
	public String getOperator(String key) {
		String op = "";
		key = msisdnNormalize(key);

		// 0=Airtel ; 1=Robi ; 2=Grameenkey ; 3=Banglalink ; 4=Teletalk

		if (key.startsWith("88017") || key.startsWith("88013") || key.startsWith("GP")) { // GP
			op = "2";
		} else if (key.startsWith("88018") || key.startsWith("RO")) { // RO
			op = "1";
		} else if (key.startsWith("88016") || key.startsWith("AT")) { // AT
			op = "0";
		} else if (key.startsWith("88019") || key.startsWith("88014") || key.startsWith("BL")) { // BL
			op = "3";
		} else if (key.startsWith("88015") || key.startsWith("TT")) { // TT
			op = "4";
		}

		return op;
	}
}