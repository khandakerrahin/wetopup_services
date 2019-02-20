package org.spider.topupservices.DBOperations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.Initializations.SecretKey;
import org.spider.topupservices.Logs.LogWriter;

/**
 * @author hafiz
 *
 */
public class UserRegistration {
	WeTopUpDS weTopUpDS;
	LogWriter logWriter;
	Configurations configurations;
	/**
	 * 
	 */
	public UserRegistration(WeTopUpDS weTopUpDS,LogWriter logWriter,Configurations configurations) {
//		weTopUpDS = new WeTopUpDS();
		this.weTopUpDS=weTopUpDS;
		this.logWriter=logWriter;
		this.configurations=configurations;
	}
	/**
	 * If msisdn starts with 0, prepends 88.
	 * If msisdn starts with 880 or any other number, returns the String
	 * @param msisdn
	 * @return msisdn of the format 8801xx
	 */
	private String msisdnNormalize(String msisdn) {
		if(msisdn.startsWith("0")) {
			msisdn="88"+msisdn;
		}
		return msisdn;
	}
	
	/**
	 * 
	 * @param userId
	 * @return 0:Successfully Inserted
	 * 
	 */
	public String insertToCustomerBalanceTable(String userId) {
		String retval= "-1";
		double balance=0.0;
		String sql="INSERT INTO user_balance (user_id,balance) VALUES (?, ?)";
		try {
			weTopUpDS.prepareStatement(sql,true);
			weTopUpDS.getPreparedStatement().setString(1, userId);
			weTopUpDS.getPreparedStatement().setDouble(2, balance);

			retval="0:Successfully Inserted";
			weTopUpDS.execute();
		}catch(Exception e) {				
			e.printStackTrace();
		}finally {
			try {
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}			
		return retval;		
	}

	/**
	 * 
	 * @param userId
	 * @return 0:Successfully Inserted
	 * -1 failed
	 */
	public String insertToTblChargingTable(String userId) {
		String retval= "-1";		
		String sql="INSERT INTO tbl_charging (user_id) VALUES (?)";
		try {
			weTopUpDS.prepareStatement(sql,true);
			weTopUpDS.getPreparedStatement().setString(1, userId);

			retval="0:Successfully Inserted";
			weTopUpDS.execute();
		}catch(Exception e) {				
			e.printStackTrace();
		}finally {
			try {
				weTopUpDS.closePreparedStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}			
		return retval;		
	}
	
	/**
	 * 
	 * @param jsonDecoder UserName,email,phone,password,custodianName,address,city,postcode
	 * @return 0:Successfully Inserted
	 * <br>1:User with the email address exists
	 * <br>2:Inserting organization details failed
	 * <br>11:Inserting user credentials failed
	 * <br>E:JSON string invalid
	 * <br>-1:Default Error Code
	 * <br>-2:SQLException
	 * <br>-3:General Exception
	 * <br>-4:SQLException while closing connection
	 */
	public JsonEncoder registerNewUser(JsonDecoder jsonDecoder) {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode="-1";//default errorCode
		String errorMessage = "default error message.";

		//user_name, phone, user_email, user_type, password
		
		String sqlInsertusers_info="INSERT INTO users_info("
				+ "user_name,user_email,user_type,status,phone,key_seed,passwd_enc)" 
				+ "VALUES" 
				+ "(?,?,'Customer',1,?,?,AES_ENCRYPT(?,concat_ws('',?,?,?,?)))";

		String userId="-1";
		try {
			//json: username,email,phone,password
			weTopUpDS.prepareStatement(sqlInsertusers_info,true);
			String keySeed=jsonDecoder.getJsonObject().getString("email")+this.msisdnNormalize(jsonDecoder.getEString("phone"));
			weTopUpDS.getPreparedStatement().setString(1, jsonDecoder.getJsonObject().getString("username"));
			weTopUpDS.getPreparedStatement().setString(2, jsonDecoder.getJsonObject().getString("email"));
			weTopUpDS.getPreparedStatement().setString(3, this.msisdnNormalize(jsonDecoder.getEString("phone")));
			weTopUpDS.getPreparedStatement().setString(4, keySeed);//key_seed
			weTopUpDS.getPreparedStatement().setString(5, jsonDecoder.getJsonObject().getString("password"));//AES encrypt password
			weTopUpDS.getPreparedStatement().setString(6, SecretKey.SECRETKEY);//key
			weTopUpDS.getPreparedStatement().setString(7, keySeed);
			weTopUpDS.getPreparedStatement().setString(8, keySeed);
			weTopUpDS.getPreparedStatement().setString(9, keySeed);
			boolean insertSuccess=false;
			try{ 
				weTopUpDS.execute();
				insertSuccess=true;
				
				userId=getUserId();
				if(!userId.equalsIgnoreCase("-1")) { //not -1 means user created successfully
					insertToCustomerBalanceTable(userId);
					insertToTblChargingTable(userId);

					errorCode = "0";
					errorMessage = "Successfully registered.";
				}else {
					LogWriter.LOGGER.info("user insert/create failed");

					errorCode = "-1";
					errorMessage = "failed to register user.";
				}
			}catch(SQLIntegrityConstraintViolationException de) {
				errorCode = "1";
				errorMessage = "User with the email address or phone number exists.";
				LogWriter.LOGGER.info("SQLIntegrityConstraintViolationException:"+de.getMessage());
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("rs:SIVE");
				this.logWriter.appendAdditionalInfo(errorCode);
			}catch(SQLException e) {
				errorCode = "11";
				errorMessage = "SQLException.";
				LogWriter.LOGGER.severe("SQLException"+e.getMessage());
				this.logWriter.setStatus(0);
				this.logWriter.appendLog("rs:SE");
				this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
			}
			LogWriter.LOGGER.info("UserID:"+userId);
			
		}catch(SQLException e){
			errorCode= "11";
			errorMessage = "SQLException.";
			LogWriter.LOGGER.severe(e.getMessage());
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("rs:SE11");
			this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
		}catch(Exception e){
			errorCode= "-3";
			errorMessage = "General Exception.";
			LogWriter.LOGGER.severe(e.getMessage());
//			e.printStackTrace();
			this.logWriter.setStatus(0);
			this.logWriter.appendLog("rs:E-3");
			this.logWriter.appendAdditionalInfo("UserReg.registerNewUser():"+e.getMessage());
		}
		jsonEncoder.addElement("ErrorCode", errorCode);
		jsonEncoder.addElement("ErrorMessage", errorMessage);
		jsonEncoder.buildJsonObject();
		
		return jsonEncoder;
	}
	/**
	 * 
	 * @return
	 * @throws SQLException
	 */
	private String getUserId() throws SQLException {
		String retval="-1";
		ResultSet rs=weTopUpDS.getGeneratedKeys();
		if(rs.next()) {
			retval=rs.getString(1);
		}
		return retval;
	}
	@SuppressWarnings("unused")
	private String getUserIdFromSequence(WeTopUpDS weTopUpDS) throws SQLException {
		String retval="-1";
		String sqlSequence="SELECT LAST_INSERT_ID()";
		weTopUpDS.prepareStatement(sqlSequence);
		ResultSet rs=weTopUpDS.executeQuery();
		if(rs.next()) {
			retval=rs.getString(1);
		}
		weTopUpDS.closePreparedStatement();		
		return retval;
	}

}
