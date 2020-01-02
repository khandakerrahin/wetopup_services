package org.spider.topupservices.Logs;

import java.sql.SQLException;
import java.util.logging.Logger;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;



/**
 * Class to set and insert the log into app_class
 * <br>Members are the fields of table app_log
 * @author shaker
 *
 */
public class LogWriter {
	public static final Logger LOGGER = Logger.getLogger(LogWriter.class.getName());
	WeTopUpDS dsConn;
	/* All the fields from table app_log */
	//action,user_id,log,status,channel
	//
	String action;
	String userId;
	String log;
	int status;
	String channel;
	String trx_id;
	String additionalInfo;
	String target;
	String inputParameters;
	String response;
	String appID = "0"; // for TopUpDBServices
	
	boolean force;
	private String subString(String str,int endIndex){
		return NullPointerExceptionHandler.isNullOrEmpty(str)?"":str.substring(0, str.length()<endIndex?str.length():endIndex);
	}
	public LogWriter(boolean force){
		this.dsConn=null;
		this.clear(force);
	}
	public LogWriter(boolean force,WeTopUpDS dsConn){
		this.dsConn=dsConn;
		this.clear(force);
	}
	/**
	 * Clears the log to default value as in the constructor.
	 */
	public void clear(){
		this.clear(this.force);
	}
	public void clear(boolean force){
		this.action       	="";
		this.userId			="";
		this.log            ="";
		this.status         =0;
		this.channel        ="";
		this.additionalInfo ="";
		this.target			="";
		this.inputParameters="";
		this.trx_id			="";
		this.response		="";
		this.force=force;
	}
	/**
	 * Changes any null input to some default value.
	 */
	private void handleNull(){
		this.action			=NullPointerExceptionHandler.isNullOrEmpty(this.action)?"":this.action;
		this.userId			=NullPointerExceptionHandler.isNullOrEmpty(this.userId)?"":this.userId;
		this.log            =NullPointerExceptionHandler.isNullOrEmpty(this.log)?"":this.log;
		this.status         =NullPointerExceptionHandler.isNullOrEmpty(this.status)?0:this.status;
		this.channel        =NullPointerExceptionHandler.isNullOrEmpty(this.channel)?"":this.channel;
		this.additionalInfo =NullPointerExceptionHandler.isNullOrEmpty(this.additionalInfo)?"":this.additionalInfo;
		this.target        	=NullPointerExceptionHandler.isNullOrEmpty(this.target)?"":this.target;
		this.inputParameters=NullPointerExceptionHandler.isNullOrEmpty(this.inputParameters)?"":this.inputParameters;
		this.trx_id       =NullPointerExceptionHandler.isNullOrEmpty(this.trx_id)?"":this.trx_id;
		this.response       =NullPointerExceptionHandler.isNullOrEmpty(this.response)?"":this.response;
		
		
//		this.action			=NullPointerExceptionHandler.isNullOrEmpty(this.action)?"null":this.action;
//		this.userId			=NullPointerExceptionHandler.isNullOrEmpty(this.userId)?"null":this.userId;
//		this.log            =NullPointerExceptionHandler.isNullOrEmpty(this.log)?"null":this.log;
//		this.status         =NullPointerExceptionHandler.isNullOrEmpty(this.status)?"null":this.status;
//		this.channel        =NullPointerExceptionHandler.isNullOrEmpty(this.channel)?"null":this.channel;
//		this.additionalInfo =NullPointerExceptionHandler.isNullOrEmpty(this.additionalInfo)?"null":this.additionalInfo;
//		this.target        	=NullPointerExceptionHandler.isNullOrEmpty(this.target)?"null":this.target;
//		this.inputParameters=NullPointerExceptionHandler.isNullOrEmpty(this.inputParameters)?"null":this.inputParameters;
//		this.response       =NullPointerExceptionHandler.isNullOrEmpty(this.response)?"null":this.response;
	}
	/**
	 * Handles null input and 
	 * <br>Truncates string input to their length bounds in table app_log 
	 */
	private void validateVariableBounds(){
		handleNull();
		
		this.action			=subString(this.action			,255);
		this.userId			=subString(this.userId			,45);
		this.log            =subString(this.log				,1000);
//		this.log            =subString(log.replaceAll("'", "''")          ,1000);
//		this.status         =subString(this.status			,4);
		this.channel        =subString(this.channel			,60);
		this.additionalInfo =subString(this.additionalInfo	,2000);
//		this.additionalInfo =subString(additionalInfo.replaceAll("'", "''")          ,2000);
		this.target        	=subString(this.target			,20);
		this.inputParameters=subString(this.inputParameters	,1000);
		this.trx_id			=subString(this.trx_id	,20);
		this.response		=subString(this.response		,1000);
		
	}

	/**
	 * Inserts the log into table app_log after calling validateVariableBounds()
	 * @return true if insert successful; false otherwise
	 */
	public boolean flush() {
		return flush(this.dsConn);
	}
	/**
	 * Inserts the log into table app_log after calling validateVariableBounds()
	 * @param dsConn datasource to use for inserting the log
	 * @return true if insert successful; false otherwise
	 */
	public boolean flush(WeTopUpDS dsConn) {
		boolean retval=false;
		this.validateVariableBounds();
//		SimpleDataSource sData = new SimpleDataSource(org.Banglalink.InhouseUtility.dataSource.DSInfo.getDS_INPATHS_NEW());
		String sql = "insert into app_log"
				+"(action,user_id,log,status,channel,additional_info,target,input_parameters,response,appID,trx_id)"
				+" values(?,?,?,?,?,?,?,?,?,?,?)";
		
//		LOGGER.info("TopUpServices: "+"LogWriter Sql:"+sql);
		try {
			dsConn.prepareStatement(sql,false);
			dsConn.setString(1, this.action);
			dsConn.setString(2, this.userId);
			dsConn.setString(3, this.log);
			dsConn.getPreparedStatement().setInt(4, this.status);
			dsConn.setString(5, this.channel);
			dsConn.setString(6, this.additionalInfo);
			dsConn.setString(7, this.target);
			dsConn.setString(8, this.inputParameters);
			dsConn.setString(9, this.response);
			dsConn.setString(10, this.appID);
			dsConn.setString(11, this.trx_id);

			dsConn.execute();
			if(dsConn.getConnection() != null) dsConn.closePreparedStatement();
			retval=true;
		} catch (SQLException e) {
			if(dsConn.getConnection() != null) {
				try {
					dsConn.closePreparedStatement();
				} catch (SQLException e1) {
					LOGGER.severe(e1.getMessage());
				}
			}
			LOGGER.severe("TopUpServices: "+"LogWriter.flush() SQLException:"+e.getMessage());
		}catch (Exception e) {
			e.printStackTrace();
			if(dsConn.getConnection() != null) {
				try {
					dsConn.closePreparedStatement();
				} catch (SQLException e1) {
					LOGGER.severe(e1.getMessage());
				}
			}
			LOGGER.severe("TopUpServices: "+"LogWriter.flush() Exception:"+e.getMessage());
		}
		LOGGER.info("TopUpServices: "+"LogWriter"+"flush() Response:"+retval);
		return retval;
	}

	public void setTrxID(String trx_id) {
		this.trx_id = trx_id;
	}
	
	/**
	 * @param userId the userId to set
	 */
	public void setTarget(String target) {
		this.target = target;
	}
	/**
	 * @param userId the userId to set
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}
	/**
	 * @param status the status to set
	 */
	public void setStatus(int status) {
		this.status = status;
	}
	/**
	 * @param channel the channel to set
	 */
	public void setChannel(String channel) {
		this.channel = channel;
	}
	/**
	 * @param log the log to set
	 * Appends to log
	 * If first log, ""+newLog
	 * else log=log+log
	 */
	public void appendLog(String newLog) {
		this.log = NullPointerExceptionHandler.isNullOrEmpty(this.log)?newLog:this.log+"|"+newLog;
	}
	/**
	 * @param action the action to set
	 */
	public void setAction(String action) {
		this.action = action;
	}
	
	/**
	 * @param force the force to set
	 */
	public void setForce(boolean force) {
		this.force = force;
	}
	
	/**
	 * @param additionalInfo the additionalInfo to set
	 * Appends to additionalInfo
	 * If first additionalInfo, ""+newAdditionalInfo
	 * else additionalInfo=additionalInfo+additionalInfo
	 */
	public void appendAdditionalInfo(String additionalInfo) {
		this.additionalInfo = NullPointerExceptionHandler.isNullOrEmpty(this.additionalInfo)?additionalInfo:this.additionalInfo+"||"+additionalInfo;
	}
	/**
	 * @param inputParameters the inputParameter to set.
	 * Appends to log
	 * If first inputParameters, ""+newInputParameters
	 * else inputParameters=inputParameters+inputParameters
	 */
	public void appendInputParameters(String inputParameters) {
		this.inputParameters = NullPointerExceptionHandler.isNullOrEmpty(this.inputParameters)?inputParameters:this.inputParameters+"|"+inputParameters;
	}
	
	public void setInputParameters(String inputParameters) {
		this.inputParameters = inputParameters;
	}
	/**
	 * 
	 * @param response
	 */
	public void setResponse(String response) {
		this.response = response;
	}
}
