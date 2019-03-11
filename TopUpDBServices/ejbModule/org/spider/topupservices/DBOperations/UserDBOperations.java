/**
 * 
 */
package org.spider.topupservices.DBOperations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Engine.JsonEncoder;
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

	public JsonEncoder insertTransaction(String user_id, String amount, String trx_id, String trx_type, String operator,
			String opType, String payee_name, String payee_phone, String payee_email, String remarks, String test) {
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

		payee_phone = msisdnNormalize(payee_phone);
		
		if (NullPointerExceptionHandler.isNullOrEmpty(user_id) || NullPointerExceptionHandler.isNullOrEmpty(amount)
				|| NullPointerExceptionHandler.isNullOrEmpty(trx_id)
				|| NullPointerExceptionHandler.isNullOrEmpty(trx_type)) {
			trxErrorCode = "5";
			trxErrorMessage = "Missing one or more parameters for insertTransactionLog.";
		} else {
			if (NullPointerExceptionHandler.isNullOrEmpty(operator)) {
				operator = "10"; // for balance
			}

			if (trx_type.equals("0") || trx_type.equals("2")) {
				payment_method = "0";
			} else {
				payment_method = "1";
				accessKey = fetchAccessKey(operator, test);

				LogWriter.LOGGER.info("accessKey : " + accessKey);
			}
			LogWriter.LOGGER.info("payment_method : " + payment_method);

			try {
				String sqlTransactionLog = "INSERT INTO transaction_log (user_id,amount,trx_id,payment_method,additional_info,trx_type,receiver_phone) "
						+ "VALUES (?,?,?,?,?,?,?)";

				try {
					weTopUpDS.prepareStatement(sqlTransactionLog, true);
					weTopUpDS.getPreparedStatement().setString(1, user_id);
					weTopUpDS.getPreparedStatement().setString(2, amount);
					weTopUpDS.getPreparedStatement().setString(3, trx_id);
					weTopUpDS.getPreparedStatement().setString(4, payment_method);
					weTopUpDS.getPreparedStatement().setString(5, additional_info);
					weTopUpDS.getPreparedStatement().setString(6, trx_type);
					weTopUpDS.getPreparedStatement().setString(7, payee_phone);

					weTopUpDS.execute();

					trxErrorCode = "0";
					trxErrorMessage = "successfully inserted into transaction_log";

					LogWriter.LOGGER.info("trxErrorMessage : " + trxErrorMessage);

					// for Balance Transfer
					if (trx_type.equals("2")) {
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

							updateTransactionStatus(trx_id, "1", additional_info, trx_type, null,null);
							additional_info = "";

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
							
							updateTransactionStatus(trx_id, "1", additional_info, trx_type, null, null).getJsonObject().toString();
							additional_info = "";
							
						} else {
							userBalanceFlag = "0";

							boolean deductFlag = false;
							if (payment_method.equals("0") && userBalanceFlag.equals("0")) {
								
								deductFlag = deductUserBalance(user_id, Double.parseDouble(amount));
								LogWriter.LOGGER.info("balance deducted : " + deductFlag);

								additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? "": (additional_info + " | ")) + "Distributor Previous Balance: "+userBalance;
								
								
								
								if (deductFlag) {
									userBalance = getUserBalance(user_id);
									additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
											: (additional_info + " | ")) + "Balance deducted. | Distributor Updated Balance: "+userBalance;
									JsonDecoder json = new JsonDecoder(
											updateTransactionStatus(trx_id, "2", additional_info, trx_type, null, null)
													.getJsonObject().toString());
									additional_info = "";
									topUpErrorCode = json.getNString("ErrorCode");
									topUpErrorMessage = json.getNString("ErrorMessage");

									if (topUpErrorCode.equals("0")) {
									} else {
										topUpErrorCode = "14";
										topUpErrorMessage = "TRX_status update failed for User balance deduction.";

										boolean flag = false;

										flag = rechargeUserBalance(user_id, Double.parseDouble(amount));
										if (flag) {
											topUpErrorCode = "16";
											topUpErrorMessage = "update failed, balance refunded.";
											additional_info = (NullPointerExceptionHandler
													.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
													+ topUpErrorMessage+ " | Distributor Previous Balance: "+userBalance+" | Distributor Updated Balance: "+getUserBalance(user_id);

											updateTransactionStatus(trx_id, "1", additional_info, trx_type, null, null);
											additional_info = "";
										} else {
											topUpErrorCode = "17";
											topUpErrorMessage = "balance refunded, update failed.";
											additional_info = (NullPointerExceptionHandler
													.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
													+ topUpErrorMessage;

											updateTransactionStatus(trx_id, "1", additional_info, trx_type, null, null);
											additional_info = "";
										}

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
						if (operator.equals("0")) {
							operatorBalance = getBalance("1");
						} else if (operator.equals("1")) {
							operatorBalance = getBalance("2");
						} else {
							operatorBalance = getBalance("3");
						}

						if (operatorBalance > Integer.parseInt(amount)) {
							opBalanceFlag = "0";
						} else {
							opBalanceFlag = "5";
							additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
									: (additional_info + " | ")) + "Low operator balance.";
							LogWriter.LOGGER.info(additional_info);
						}

						LogWriter.LOGGER.info("operatorBalance : " + operatorBalance);

						if (userBalance < Double.parseDouble(amount)) {
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
								boolean deductFlag = false;
								if (payment_method.equals("0") && userBalanceFlag.equals("0")) {
									deductFlag = deductUserBalance(user_id, Double.parseDouble(amount));
									LogWriter.LOGGER.info("balance deducted : " + deductFlag);
									
									additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
											: (additional_info + " | ")) + "Customer Previous Balance: "+userBalance;
									
									if (deductFlag) {
										userBalance = getUserBalance(user_id);
										additional_info = (NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? ""
												: (additional_info + " | ")) + "Balance deducted. | Customer Updated Balance: "+userBalance;
										
										JsonDecoder json = new JsonDecoder(
												updateTransactionStatus(trx_id, "2", additional_info, trx_type, null, null)
														.getJsonObject().toString());
										additional_info = "";
										topUpErrorCode = json.getNString("ErrorCode");
										topUpErrorMessage = json.getNString("ErrorMessage");

										if (topUpErrorCode.equals("0")) {

										} else {
											topUpErrorCode = "14";
											topUpErrorMessage = "TRX_status update failed for User balance deduction.";

											boolean flag = false;

											flag = rechargeUserBalance(user_id, Double.parseDouble(amount));
											if (flag) {
												topUpErrorCode = "16";
												topUpErrorMessage = "update failed, balance refunded.";
												additional_info = (NullPointerExceptionHandler
														.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
														+ topUpErrorMessage+ " | Customer Previous Balance: "+userBalance+" | Customer Updated Balance: "+getUserBalance(user_id);

												updateTransactionStatus(trx_id, "1", additional_info, trx_type, null, null);
												additional_info = "";
											} else {
												topUpErrorCode = "17";
												topUpErrorMessage = "balance refunded, update failed.";
												additional_info = (NullPointerExceptionHandler
														.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
														+ topUpErrorMessage;

												updateTransactionStatus(trx_id, "1", additional_info, trx_type, null, null);
												additional_info = "";
											}

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
						topup_trx_id = topup_trx_id + RandomStringGenerator.getRandomString("abcdefghijklmnopqrstuvwxyz", 4);
						topup_trx_id = topup_trx_id + RandomStringGenerator.getRandomString("0123456789abcdefghABCDEFGHIJKLMNOPQRSTUVWXYZijklmnopqrstuvwxyz", 7);
						topup_trx_id = topup_trx_id + RandomStringGenerator.getRandomString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 2);
						
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

	/**
	 * If msisdn starts with 0, prepends 88. If msisdn starts with 880 or any other
	 * number, returns the String
	 * 
	 * @param msisdn
	 * @return msisdn of the format 8801xx
	 */
	private String msisdnNormalize(String msisdn) {
		if (msisdn.startsWith("0")) {
			msisdn = "88" + msisdn;
		}
		return msisdn;
	}

	private boolean checkRetailer(String msisdn) {
		boolean flag = false;
		String sql = "SELECT user_type FROM users_info where user_type=? and phone=?";
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, "5"); // for Retailer
			msisdn = msisdnNormalize(msisdn);
			weTopUpDS.getPreparedStatement().setString(2, msisdn);

			ResultSet rs = weTopUpDS.executeQuery();
			if (rs.next()) {
				flag = true;
			}
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (NullPointerException e) {
			LogWriter.LOGGER.severe(e.getMessage());
		} catch (Exception e) {
			if (weTopUpDS.getConnection() != null) {
				try {
					weTopUpDS.closeResultSet();
				} catch (SQLException e1) {
					LogWriter.LOGGER.severe(e1.getMessage());
				}
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

	public JsonEncoder sendEmail(String action, String key, String email, String trx_id) {
		JsonEncoder jsonEncoder = new JsonEncoder();

		String errorCode = "-1";
		String errorMessage = "General Error";

		String app_name = "we_top_up";
		String subject = "WeTopUp Recharge Service";
		String mailbody = "WeTopUp Reset Password";
		String amount = "";
		Double credit_amount = 0.0;
		String operator = "";
		String opType = "";
		String payee_phone = "";
		String time = "";
		String remarks = "";
		String payment_method = "";
		
		subject = fetchMailSubject(action);
		mailbody = fetchMailBody(action);
		LogWriter.LOGGER.info("mailbody : "+mailbody);
		
		if(action.equals("resetPassword")) {
			mailbody = mailbody.replace("replace_key_here",key);	
		}
		else if(action.equals("registrationSuccess")) {
			mailbody = mailbody.replace("replace_phone_here",key);
			mailbody = mailbody.replace("replace_email_here",email);
		}
		else if(action.equals("topupSuccess") || action.equals("topupFailed")) {
			JsonDecoder json = new JsonDecoder(getTopUpStatus(trx_id).getJsonObject().toString());
			
			remarks = json.getNString("remarks");
			amount = json.getNString("amount");
			payee_phone = json.getNString("payee_phone");
			operator = getOperatorName(json.getNString("operator"));
			opType = getOpType(json.getNString("opType"));
			time = json.getNString("response_time");
			payment_method = json.getNString("payment_method");
			
			payment_method = getPaymentMethod(payment_method);
			
			mailbody = mailbody.replace("replace_remarks_here",remarks);
			mailbody = mailbody.replace("replace_operator_here",operator);
			mailbody = mailbody.replace("replace_phone_here",payee_phone);
			mailbody = mailbody.replace("replace_amount_here",amount);
			mailbody = mailbody.replace("replace_opType_here",opType);
			mailbody = mailbody.replace("replace_time_here",time);
			mailbody = mailbody.replace("replace_trx_id_here",trx_id);
			mailbody = mailbody.replace("replace_payment_method_here",payment_method);
		}
		else{
			JsonDecoder json = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());
			
			amount = json.getNString("amount");
			credit_amount = Double.parseDouble(json.getNString("credit_amount"));
			payee_phone = json.getNString("receiver_phone");
			time = json.getNString("update_time");
			payment_method = json.getNString("payment_method");
			
			payment_method = getPaymentMethod(payment_method);
			
			mailbody = mailbody.replace("replace_remarks_here",remarks);
			mailbody = mailbody.replace("replace_operator_here",operator);
			mailbody = mailbody.replace("replace_phone_here",payee_phone);
			mailbody = mailbody.replace("replace_amount_here",amount);
			mailbody = mailbody.replace("replace_credit_amount_here",""+credit_amount);
			mailbody = mailbody.replace("replace_commission_rate_here",amount);
			mailbody = mailbody.replace("replace_commission_amount_here",amount);
			mailbody = mailbody.replace("replace_opType_here",opType);
			mailbody = mailbody.replace("replace_time_here",time);
			mailbody = mailbody.replace("replace_trx_id_here",trx_id);
			mailbody = mailbody.replace("replace_payment_method_here",payment_method);
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
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				jsonReqName = weTopUpDS.getResultSet().getString("req_file_name");
				jsonReqPath = weTopUpDS.getResultSet().getString("req_file_location");
				templateID = weTopUpDS.getResultSet().getString("templateID");
			}
			weTopUpDS.closeResultSet();
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
	
	public JsonEncoder updateTransactionStatus(String trx_id, String status, String additional_info, String trx_type,
			String LP_trx_status, String payment_method) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		LogWriter.LOGGER.info("UPDATE : " + trx_id + "	" + status + "	" + additional_info + "	" + LP_trx_status);

		String sql = "UPDATE `transaction_log` SET trx_status = ?, LP_trx_status = ?, payment_method = case when ? is not null then ? else payment_method end, additional_info = case when additional_info is null and ? is not null then ? "
				+ "when additional_info is not null and ? is not null then concat(additional_info,' | ',?) else additional_info end WHERE trx_id=?";

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
			weTopUpDS.getPreparedStatement().setString(9, trx_id);
			weTopUpDS.execute();
			weTopUpDS.closePreparedStatement();
			errorCode = "0";
			errorMessage = "Update successful.";
			additional_info = "";
		} catch (SQLException e) {
			LogWriter.LOGGER.severe(e.getMessage());
			errorCode = "11";
			errorMessage = "SQL Exception";
		}

		String balErrorCode = "-1";
		String balErrorMessage = "General Error";
		try {
			
//			for balance recharge
			if (status.equals("2") && trx_type.equals("1")) {
				// recharge balance
				JsonDecoder jd = new JsonDecoder(getSingleTransaction(trx_id).getJsonObject().toString());
				String user_id = jd.getNString("user_id");
				String amount = jd.getNString("amount");
				String balFlag = jd.getNString("bal_rec_status");
				Double userBalance = getUserBalance(user_id);

				boolean flag = false;
				LogWriter.LOGGER.info("user_id : " + user_id);
				if (balFlag.equals("0")) {
					flag = rechargeUserBalance(user_id, Double.parseDouble(amount));
					if (flag) {
						balErrorCode = "0";
						balErrorMessage = "Successfully updated balance";
						additional_info = (NullPointerExceptionHandler
								.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
								+ "Customer Previous Balance: "+userBalance+" | Customer Updated Balance: "+getUserBalance(user_id);
						
						updateBalTrxStatus(trx_id, "4", additional_info, trx_type);
						
						String phone = getUserPhone(user_id);
						String email = getUserEmail(user_id);
						
						try{
//							send email
							if(NullPointerExceptionHandler.isNullOrEmpty(email)) {
								
							}else {
								new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("balancePurchaseSuccess", phone, email, trx_id);
								updateEmailStatus(trx_id,"2");
							}
						}catch(Exception e) {
							e.printStackTrace();
						}
					} else {
						balErrorCode = "5";
						balErrorMessage = "Failed to update balance";
						updateBalTrxStatus(trx_id, "10", additional_info, trx_type);
						String phone = getUserPhone(user_id);
						String email = getUserEmail(user_id);
						
						try {
//							send email
							if(NullPointerExceptionHandler.isNullOrEmpty(email)) {
								
							}else {
								new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("balancePurchaseFailed", phone, email, trx_id);
								updateEmailStatus(trx_id,"2");
							}
						}catch(Exception e) {
							e.printStackTrace();
						}
					}
				}
				
			}

			//	for balance transfer
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

				jd = new JsonDecoder(getCommissionRate(payee_phone).getJsonObject().toString());
				
				if(jd.getNString("ErrorCode").equals("0")) {
					commissionRate = Double.parseDouble(jd.getNString("commission_rate"));
					
					commissionAmount = calculateCommissionAmount(amount,commissionRate);
					
					creditAmount = commissionAmount + amount;
					
					userBalance = getUserBalance(jd.getNString("user_id"));
					
					boolean flag = false;
					LogWriter.LOGGER.info("receiver_phone : " + payee_phone);
					if (balFlag.equals("0")) {
						
						flag = transferUserBalance(payee_phone, creditAmount);
						LogWriter.LOGGER.info("balance transfered : " + flag);
						
						if (flag) {
							balErrorCode = "0";
							
							additional_info = (NullPointerExceptionHandler
									.isNullOrEmpty(additional_info) ? "" : (additional_info + " | "))
									+ "Retailer Previous Balance: "+userBalance+" | Retailer Updated Balance: "+getUserBalance(jd.getNString("user_id"));
							
							balErrorMessage = "Successfully updated balance";
							updateBalTrxStatus(trx_id, "4", additional_info, trx_type,commissionRate,commissionAmount,creditAmount);
							
							String phone = getUserPhone(jd.getNString("user_id"));
							String email = getUserEmail(jd.getNString("user_id"));
							
							try {
//								send email
								if(NullPointerExceptionHandler.isNullOrEmpty(email)) {
									
								}else {
									new UserDBOperations(weTopUpDS,configurations,logWriter).sendEmail("balanceTransferSuccess", phone, email, trx_id);
									updateEmailStatus(trx_id,"2");
								}
							}catch(Exception e) {
								e.printStackTrace();
							}
							
						} else {
							balErrorCode = "5";
							balErrorMessage = "Failed to update balance";
							updateBalTrxStatus(trx_id, "10", additional_info, trx_type,commissionRate,commissionAmount,creditAmount);
							
//							//	send email
						}
					}
				}else {
					balErrorCode = "-4";
					balErrorMessage = "Failed to fetch commission rate";
				}
			}
			LogWriter.LOGGER.info("balErrorMessage : " + balErrorMessage);
			jsonEncoder.addElement("balErrorCode", balErrorCode);
			jsonEncoder.addElement("balErrorMessage", balErrorMessage);
		}catch(Exception e) {
			LogWriter.LOGGER.severe(e.getMessage());
			balErrorCode = "-3";
			balErrorMessage = "Failed to update balance";
			jsonEncoder.addElement("balErrorCode", balErrorCode);
			jsonEncoder.addElement("balErrorMessage", balErrorMessage);
		}
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}
	
	private boolean updateEmailStatus(String trxID, String status) {
		boolean retval = false;
		String sqlUpdateUser = "UPDATE transaction_log set notification_email=?  where trx_id=?";
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
	
	public Double calculateCommissionAmount(Double amount,Double commissionRate) {
		Double commissionAmount = 0.0;
		
		commissionAmount = amount*commissionRate/100.0;
		
		return commissionAmount;
	}
	
	public JsonEncoder getCommissionRate(String phone) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";
		
		String user_id = "";

		phone = msisdnNormalize(phone);
		
		Double commissionRate = 0.0;

		String sql = "SELECT t.regular_charge,u.user_id FROM users_info u left join tbl_charging t on u.user_id=t.user_id where u.phone=? order by u.created_at desc";

		
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, phone);
			weTopUpDS.executeQuery();
			while (weTopUpDS.getResultSet().next()) {
				commissionRate = weTopUpDS.getResultSet().getDouble(1);
				user_id = weTopUpDS.getResultSet().getString(2);
			}
			

			errorCode = "0";
			errorMessage = "fetched commissionRate successfully.";

			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			errorCode = "11";
			errorMessage = "SQL Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
		}catch (Exception e) {
			e.printStackTrace();
			errorCode = "-3";
			errorMessage = "General exception : fetch commissionRate.";
			LogWriter.LOGGER.severe(e.getMessage());
		}

		LogWriter.LOGGER.info("commissionRate :" + commissionRate);
		
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("commission_rate", ""+commissionRate);
		jsonEncoder.addElement("user_id", ""+user_id);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public JsonEncoder updateBalTrxStatus(String trx_id, String status, String additional_info, String trx_type, Double commissionRate, Double commissionAmount, Double creditAmount) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";

		LogWriter.LOGGER.info("UPDATE : " + trx_id + "	" + status + "	" + additional_info);

		String sql = "UPDATE `transaction_log` SET bal_rec_status =?, additional_info = case when additional_info is null and ? is not null then ? when additional_info is not null and ? is not null then concat(additional_info,' | ',?) else additional_info end, commission_amount=?, credit_amount=?, commission_rate=? WHERE trx_id=?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, status);
			weTopUpDS.getPreparedStatement().setString(2, additional_info);
			weTopUpDS.getPreparedStatement().setString(3, additional_info);
			weTopUpDS.getPreparedStatement().setString(4, additional_info);
			weTopUpDS.getPreparedStatement().setString(5, additional_info);
			weTopUpDS.getPreparedStatement().setDouble(6, commissionAmount);
			weTopUpDS.getPreparedStatement().setDouble(7, creditAmount);
			weTopUpDS.getPreparedStatement().setDouble(8, commissionRate);
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

		String sql = "UPDATE `transaction_log` SET bal_rec_status =?, additional_info = case when additional_info is null and ? is not null then ? when additional_info is not null and ? is not null then concat(additional_info,' | ',?) else additional_info end WHERE trx_id=?";

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
		
		List<String> templateInfo = this.configurations.getUserTemplates().containsKey(action)? this.configurations.getUserTemplates().get(action):emptyList;
		
		mailbody = (String) templateInfo.get(4);
		
		return mailbody;
	}
	
	public String getPaymentMethod(String payment_method) {
		
		if(payment_method.equals("0")) {
			payment_method = "from your Emergency balance.";
		}else if(payment_method.equals("1")) {
			payment_method = "via Lebupay.";
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
		
		List<String> templateInfo = this.configurations.getUserTemplates().containsKey(action)? this.configurations.getUserTemplates().get(action):emptyList;
		
		String subject = (String) templateInfo.get(2);
		
		return subject;
	}
	
	public JsonEncoder updatePaymentMethod(String trx_id, String payment_method) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "Update failed.";
		if (NullPointerExceptionHandler.isNullOrEmpty(trx_id)) {
			errorCode = "5";
			errorMessage = "can not update for empty trx_id.";
		} else {
			String sql = "UPDATE `transaction_log` SET payment_method =? WHERE trx_id=?";

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

	public JsonEncoder getTopUpStatus(String trx_id) {
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

		String errorCode = "-1";
		String errorMessage = "General error.";

		String sql = "SELECT t.trx_status, a.top_up_status, a.user_id, a.payee_phone, a.amount, a.operator, a.opType, a.payee_email, a.remarks, date_format(a.response_time, '%Y-%m-%d %H:%i:%S'), a.topup_trx_id, t.payment_method FROM topup_log a left join transaction_log t on t.trx_id=a.trx_id WHERE a.trx_id=?";

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
				topup_trx_id = weTopUpDS.getResultSet().getString(11);
				payment_method = weTopUpDS.getResultSet().getString(12);
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
		jsonEncoder.addElement("topup_trx_id", topup_trx_id);
		jsonEncoder.addElement("trx_status", trx_status);
		jsonEncoder.addElement("top_up_status", top_up_status);
		jsonEncoder.addElement("user_id", user_id);
		jsonEncoder.addElement("payment_method", payment_method);
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
		Double commission_amount= 0.0;
		Double credit_amount = 0.0;
		Double commission_rate = 0.0;
		String notification_email = "";

		String errorCode = "-1";
		String errorMessage = "General error.";

		String sql = "SELECT user_id, trx_id, amount, receiver_phone, trx_status, bal_rec_status, date_format(insert_time, '%Y-%m-%d %H:%i:%S'),date_format(update_time, '%Y-%m-%d %H:%i:%S') , additional_info, "
				+ "payment_method, card_brand, card_number, bank, bkash_payment_number, billing_name, trx_type,commission_amount, credit_amount, commission_rate,notification_email FROM transaction_log where trx_id=?";

		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, trx_id);
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				user_id = weTopUpDS.getResultSet().getString(1);
				trx_id = weTopUpDS.getResultSet().getString(2);
				amount = weTopUpDS.getResultSet().getString(3);
				receiver_phone = weTopUpDS.getResultSet().getString(4);
				trx_status = weTopUpDS.getResultSet().getString(5);
				bal_rec_status = weTopUpDS.getResultSet().getString(6);
				insert_time = weTopUpDS.getResultSet().getString(7);
				update_time = weTopUpDS.getResultSet().getString(8);
				additional_info = weTopUpDS.getResultSet().getString(9);
				payment_method = weTopUpDS.getResultSet().getString(10);
				card_brand = weTopUpDS.getResultSet().getString(11);
				card_number = weTopUpDS.getResultSet().getString(12);
				bank = weTopUpDS.getResultSet().getString(13);
				bkash_payment_number = weTopUpDS.getResultSet().getString(14);
				billing_name = weTopUpDS.getResultSet().getString(15);
				trx_type = weTopUpDS.getResultSet().getString(16);
				commission_amount = weTopUpDS.getResultSet().getDouble(17);
				credit_amount = weTopUpDS.getResultSet().getDouble(18);
				commission_rate = weTopUpDS.getResultSet().getDouble(19);
				notification_email = weTopUpDS.getResultSet().getString(20);

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
		jsonEncoder.addElement("amount", amount);
		jsonEncoder.addElement("receiver_phone",
				NullPointerExceptionHandler.isNullOrEmpty(receiver_phone) ? "" : receiver_phone);
		jsonEncoder.addElement("trx_status", trx_status);
		jsonEncoder.addElement("bal_rec_status", bal_rec_status);
		jsonEncoder.addElement("insert_time",
				NullPointerExceptionHandler.isNullOrEmpty(insert_time) ? "" : insert_time);
		jsonEncoder.addElement("update_time",
				NullPointerExceptionHandler.isNullOrEmpty(update_time) ? "" : update_time);
		jsonEncoder.addElement("additional_info",
				NullPointerExceptionHandler.isNullOrEmpty(additional_info) ? "" : additional_info);
		jsonEncoder.addElement("payment_method", payment_method);
		jsonEncoder.addElement("card_brand", NullPointerExceptionHandler.isNullOrEmpty(card_brand) ? "" : card_brand);
		jsonEncoder.addElement("card_number",
				NullPointerExceptionHandler.isNullOrEmpty(card_number) ? "" : card_number);
		jsonEncoder.addElement("bank", NullPointerExceptionHandler.isNullOrEmpty(bank) ? "" : bank);
		jsonEncoder.addElement("bkash_payment_number",
				NullPointerExceptionHandler.isNullOrEmpty(bkash_payment_number) ? "" : bkash_payment_number);
		jsonEncoder.addElement("billing_name",
				NullPointerExceptionHandler.isNullOrEmpty(billing_name) ? "" : billing_name);
		jsonEncoder.addElement("commission_amount", ""+commission_amount);
		jsonEncoder.addElement("credit_amount", ""+credit_amount);
		jsonEncoder.addElement("commission_rate", ""+commission_rate);
		jsonEncoder.addElement("notification_email", ""+notification_email);
		jsonEncoder.addElement("trx_type", trx_type);

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
			weTopUpDS.executeQuery();
			if (weTopUpDS.getResultSet().next()) {
				operator = weTopUpDS.getResultSet().getString(1);
			}
			weTopUpDS.closeResultSet();
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

	public JsonEncoder fetchRetailerList(String userID) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String retailer_list = "";

		String sql = "SELECT u.user_name,u.phone,u.user_email,u.address,t.regular_charge,date_format(u.created_at, '%Y-%m-%d %H:%i:%S'),u.balance,u.dp_img,u.doc_img_01,u.doc_img_02,u.doc_img_03 FROM users_info u left join tbl_charging t on u.user_id=t.user_id where distributor_id=? order by u.created_at desc";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			weTopUpDS.executeQuery();
			while (weTopUpDS.getResultSet().next()) {
				// user_name, phone, user_email, address, regular_charge, date_created_at, balance, dp_img, doc_img_01, doc_img_02, doc_img_03
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(1) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(2) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(3) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(4) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(5) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(6) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(7) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(8) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(9) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(10) + "\"" + ";";
				retailer_list += "\"" + weTopUpDS.getResultSet().getString(11) + "\"" + "|";
			}
			int lio = retailer_list.lastIndexOf("|");
			if (lio > 0)
				retailer_list = retailer_list.substring(0, lio);

			errorCode = "0";
			errorMessage = "fetched retailer list successfully.";

			weTopUpDS.closeResultSet();
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

	public JsonEncoder fetchTrxHistory(String userID, String userType, String phone) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1";
		String errorMessage = "General error.";

		String mode = "";
		
		String trx_history = "";
		String sql = "";
		
		if (NullPointerExceptionHandler.isNullOrEmpty(userID) || NullPointerExceptionHandler.isNullOrEmpty(userType)
				|| NullPointerExceptionHandler.isNullOrEmpty(phone)) {
			errorCode = "5";
			errorMessage = "Missing one or more parameters.";
		}else {
			if(userType.equals("5")) {
				mode = msisdnNormalize(phone);
				sql = "SELECT trx_id, amount, trx_status, LP_trx_status, trx_type, insert_time, user_id FROM transaction_log where trx_type in (?,?) and receiver_phone=? order by insert_time desc";
			}else {
				mode = userID;
				sql = "SELECT trx_id, amount, trx_status, LP_trx_status, trx_type, insert_time, receiver_phone  FROM transaction_log where trx_type in (?,?) and user_id=? order by insert_time desc";
			}
			try {
				weTopUpDS.prepareStatement(sql);
				weTopUpDS.getPreparedStatement().setString(1, "1"); // for balance recharge
				weTopUpDS.getPreparedStatement().setString(2, "2"); // for balance transfer
				weTopUpDS.getPreparedStatement().setString(3, mode);
				weTopUpDS.executeQuery();
				ResultSet rs = weTopUpDS.getResultSet();
				while (rs.next()) {
					//	trx_id, amount, trx_status, LP_trx_status, trx_type, insert_time, user_id
					trx_history += "\"" + rs.getString(1) + "\"" + ",";
					trx_history += "\"" + rs.getString(2) + "\"" + ",";
					trx_history += "\"" + rs.getString(3) + "\"" + ",";
					trx_history += "\"" + rs.getString(4) + "\"" + ",";
					trx_history += "\"" + rs.getString(5) + "\"" + ",";
					trx_history += "\"" + rs.getString(6) + "\"" + ",";
					if(userType.equals("5")) {
						trx_history += "\"" + getUserNameByID(rs.getString(7)) + "\"" + "|";
					}else {
						trx_history += "\"" + getUserNameByPhone(rs.getString(7)) + "\"" + "|";
					}
					
					
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

		
		

		LogWriter.LOGGER.info("trx_history :" + trx_history);
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

		String sql = "SELECT a.trx_id, a.payee_phone, a.amount, t.trx_status, t.LP_trx_status, a.top_up_status, a.insert_time, t.payment_method FROM topup_log a left join transaction_log t on a.trx_id=t.trx_id WHERE a.user_id=? order by a.insert_time desc";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setString(1, userID);
			weTopUpDS.executeQuery();
			while (weTopUpDS.getResultSet().next()) {
				// trx_id,payee_phone,amount,trx_status,LP_trx_status,top_up_status,insert_time,payment_method
				trx_history += "\"" + weTopUpDS.getResultSet().getString(1) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(2) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(3) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(4) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(5) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(6) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(7) + "\"" + ",";
				trx_history += "\"" + weTopUpDS.getResultSet().getString(8) + "\"" + "|";
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

		LogWriter.LOGGER.info("trx_history :" + trx_history);
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.addElement("userID", userID);
		jsonEncoder.addElement("trx_history", trx_history);

		jsonEncoder.buildJsonObject();
		return jsonEncoder;
	}

	public double getBalance(String id) {
		// id = 1 = airtel
		// id = 2 = robi
		// id = 3 = paywell
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

		return userEmail;
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

		return userPhone;
	}
	public double getUserBalance(String user_id) {
		double balance = 0.0;
		String sql = "SELECT balance FROM users_info where user_id=?";

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

	public boolean transferUserBalance(String payee_phone, Double amount) {
		boolean flag = false;
		//	String sql = "UPDATE users_info SET balance = FORMAT((balance + ?),2) WHERE phone=?";
		String sql = "UPDATE users_info SET balance =  round((balance + ?),6) WHERE phone=?";
		
		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setDouble(1, amount);
			weTopUpDS.getPreparedStatement().setString(2, payee_phone);
			weTopUpDS.execute();
			flag = true;
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}

	public boolean deductUserBalance(String user_id, Double amount) {
		boolean flag = false;
		String sql = "UPDATE users_info SET balance = round((balance - ?),6) WHERE user_id = ?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setDouble(1, amount);
			weTopUpDS.getPreparedStatement().setString(2, user_id);
			weTopUpDS.execute();
			flag = true;
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}

	public boolean rechargeUserBalance(String user_id, Double amount) {
		boolean flag = false;
		String sql = "UPDATE users_info SET balance = round((balance + ?),6) WHERE user_id = ?";

		try {
			weTopUpDS.prepareStatement(sql);
			weTopUpDS.getPreparedStatement().setDouble(1, amount);
			weTopUpDS.getPreparedStatement().setString(2, user_id);
			weTopUpDS.execute();
			flag = true;
			weTopUpDS.closeResultSet();
			weTopUpDS.closePreparedStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}

		return flag;
	}
}