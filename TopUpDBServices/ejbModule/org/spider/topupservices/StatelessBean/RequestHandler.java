package org.spider.topupservices.StatelessBean; 

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.annotation.PostConstruct; 
import javax.ejb.Startup;
import javax.ejb.Stateless;
import javax.jms.JMSException;
import javax.jms.MapMessage;

import org.spider.topupservices.Engine.JsonEncoder;
import org.spider.topupservices.Engine.JsonDecoder;
import org.spider.topupservices.Engine.LoginProcessor;
import org.spider.topupservices.Engine.RegistrationProcessor;
import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;
import org.spider.topupservices.Engine.UserOperations;



/**
 * Session Bean implementation class
 * @author shaker
 * @implements RequestHandlerLocal session interface.
 * @see RequestHandlerLocal
 */
@Stateless
@Startup
//@Interceptors(value = org.Spider.Utility.monitor.MessageMonitor.class)
public class RequestHandler implements RequestHandlerLocal {

	public Configurations configurations;
	LogWriter logWriter;
	String replyAddr="spidertopup";
	String appName="spidertopup";
	String channel;
	public RequestHandler() {
	}

	@PostConstruct
	public void loadConfiguration() {
		try {
		} catch (Exception ex) {
			LogWriter.LOGGER.severe(ex.getMessage());
		}
	}

	/**
	 * Implementation method to process new request.
	 * @param msg
	 * @param configurations
	 * @param force
	 * @return The result of the processing. String value.
	 * @throws JMSException
	 * @throws Exception
	 * @see RequestHandlerLocal
	 */
	@Override
	public String processNewRequest(MapMessage msg, Configurations configurations, boolean forceLogWrite) throws JMSException, Exception  {
		JsonEncoder jsonEncoder = new JsonEncoder();
		String errorCode = "-1"; 
		String errorMessage = "";
		String retval="";
		//request example  
		//login https://localhost:8443/HttpReceiver/HttpReceiver?destinationName=fees.school&destinationType=queue&clientid=fees.school&target=ENGINE&LoadConf=N&message={%20%22username%22:%22t1@sp.com%22,%20%22password%22:%22specialt1pass%22,%20%22mode%22:%221%22}&reply=true&action=login
		//school registration: https://localhost:8443/HttpReceiver/HttpReceiver?destinationName=fees.school&destinationType=queue&clientid=fees.school&target=ENGINE&LoadConf=N&message={%20%22schoolName%22:%22Skola%201%22,%22email%22:%22spiderco@sdxb.com%22,%22phone%22:%228801912345678%22,%22password%22:%22spidercom%22,%22custodianName%22:%22SpiderCom%22,%22address%22:%2210A%20Dhanmondi%22,%22city%22:%22Dhaka%22,%22postcode%22:%221209%22}&reply=true&action=registerSchool
		WeTopUpDS dsConn=new WeTopUpDS();
		
		this.logWriter=new LogWriter(forceLogWrite);
		String message 	=	NullPointerExceptionHandler.isNullOrEmpty(msg.getString("message"))?"":msg.getString("message");
		String action 	= 	NullPointerExceptionHandler.isNullOrEmpty(msg.getString("action")) ?"":msg.getString("action");
		String messageBody=	NullPointerExceptionHandler.isNullOrEmpty(msg.getString("body"))?"":msg.getString("body");
		String isTest 	= 	NullPointerExceptionHandler.isNullOrEmpty(msg.getString("isTest"))?"":msg.getString("isTest");
		String src   	= 	NullPointerExceptionHandler.isNullOrEmpty(msg.getString("src"))?"":msg.getString("src");
		String target  	= 	NullPointerExceptionHandler.isNullOrEmpty(msg.getString("target"))?"":msg.getString("target");
		String traceON 	= 	NullPointerExceptionHandler.isNullOrEmpty(msg.getString("traceON"))?"":msg.getString("traceON");
		String channel 	= 	NullPointerExceptionHandler.isNullOrEmpty(msg.getString("channel"))?"default":msg.getString("channel");
		boolean isTestEnv = false;
		this.logWriter.setChannel(channel);
		this.logWriter.setTarget(target);
//		this.logWriter.setSource(src);
		this.logWriter.setAction(action);
		this.logWriter.setStatus(0);
		this.logWriter.appendInputParameters("m:"+message);
		this.logWriter.appendInputParameters("b:"+messageBody);
		this.logWriter.appendInputParameters("u:"+ (NullPointerExceptionHandler.isNullOrEmpty(msg.getString("userId"))?"":msg.getString("userId")));
		this.logWriter.setChannel(channel);
		this.logWriter.setTarget(target);
		this.logWriter.setTrxID(new UserOperations(dsConn,this.logWriter,this.configurations).getTrxId(message,messageBody));
		this.configurations = configurations;
		LogWriter.LOGGER.info("Message :"+message);
		LogWriter.LOGGER.info("Message Body :"+messageBody);
		
		try {
			retval=new UserOperations(dsConn,this.logWriter,this.configurations).verifyUser(message,messageBody);
			if(new JsonDecoder(retval).getNString("ErrorCode").equals("0")) {
				LogWriter.LOGGER.info("application authentication succesful.");
				LogWriter.LOGGER.info("action : "+action.toUpperCase());
				switch(action.toUpperCase()) {
				case "LOGIN":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).processLogin(message,messageBody);
					break;
				case "REGISTER":
					this.logWriter.setUserId(NullPointerExceptionHandler.isNullOrEmpty(msg.getString("userId"))?"":msg.getString("userId"));
					retval=new RegistrationProcessor(dsConn,this.logWriter,this.configurations).processUserRegistration(message,messageBody);
					break;
				case "REGISTERRETAILER":
					this.logWriter.setUserId(NullPointerExceptionHandler.isNullOrEmpty(msg.getString("userId"))?"":msg.getString("userId"));
					retval=new RegistrationProcessor(dsConn,this.logWriter,this.configurations).processRetailerRegistration(message,messageBody);
					break;
				case "REMOVERETAILER":
					this.logWriter.setUserId(NullPointerExceptionHandler.isNullOrEmpty(msg.getString("userId"))?"":msg.getString("userId"));
					retval=new RegistrationProcessor(dsConn,this.logWriter,this.configurations).removeRetailer(message,messageBody);
					break;
				case "CHECKUSER":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).checkUser(message,messageBody);
					break;
				case "FETCHUSEREMAIL":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).fetchUserEmail(message,messageBody);
					break;
				case "CHECKUSERENTRY":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).checkUserEntry(message,messageBody);
					break;
				case "UPDATEKEY":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).updateKey(message,messageBody);
					break;
				case "UPDATEPASSWORD":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).updatePassword(message,messageBody);
					break;
				case "CHANGEPASSWORD":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).changePassword(message,messageBody);
					break;
				case "FETCHUSERBYKEY":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).fetchUserByKey(message,messageBody);
					break;
				case "FETCHUSERBALANCE":
					retval=new LoginProcessor(dsConn,this.logWriter,this.configurations).fetchUserBalance(message,messageBody);
					break;
//				case "INSERTTRANSACTION":
//					retval=new UserOperations(dsConn,this.logWriter,this.configurations).insertTransaction(message,messageBody);
//					break;
				case "INSERTTRANSACTION":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).insertTransaction(message,messageBody);
					break;
				case "SETPAYMENTMETHOD":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).setPaymentMethod(message,messageBody);
					break;
				case "UPDATETOPUPSTATUS":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).updateTopUpStatus(message,messageBody);
					break;
				case "UPDATEPAYMENTSTATUS":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).updatePaymentStatus(message,messageBody);
					break;
				case "FETCHTRANSACTIONHISTORY":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).fetchTransactionHistory(message,messageBody);
					break;
				case "FETCHRETAILERLIST":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).fetchRetailerList(message,messageBody);
					break;
				case "FETCHTOPUPHISTORY":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).fetchTopUpHistory(message,messageBody);
					break;
				case "GETTOPUPSTATUS":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).getTopUpStatus(message,messageBody);
					break;
				case "FETCHSINGLETRANSACTION":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).fetchSingleTransaction(message,messageBody);
					break;
				case "FETCHACCESSKEY":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).fetchAccessKey(message,messageBody);
					break;
				case "SENDEMAIL":
					retval=new UserOperations(dsConn,this.logWriter,this.configurations).sendEmail(message,messageBody);
					break;
				case "CANCELTOPUP":
					retval="cancelled by user.";
					break;	
				default:
					jsonEncoder.addElement("ErrorCode", "-9");
					jsonEncoder.addElement("ErrorMessage", "Invalid action");
					jsonEncoder.buildJsonObject();
					retval=jsonEncoder.getJsonObject().toString();
					break;
				}
			}else {
				LogWriter.LOGGER.info("application authentication failed.");
			}
		}
		finally{
			LogWriter.LOGGER.info("retVal : "+retval);
			if(dsConn.getConnection() != null){
				try { 
				this.logWriter.setStatus(Integer.parseInt(new JsonDecoder(retval).getErrorCode())); }catch(NumberFormatException nfe) {}
				this.logWriter.setResponse(retval);
				this.logWriter.flush(dsConn);
				try {
					dsConn.getConnection().close();
				} catch (SQLException e) {
					LogWriter.LOGGER.severe(e.getMessage());
				}
			}
		}
		
		return retval;
	}
	
	/**
	 * Gets the date today T + i days
	 * @param tPlusD
	 * @return date in yyyyMMdd format
	 */
	public String getDate(int tPlusD) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, tPlusD);
		java.util.Date dt = cal.getTime();
		SimpleDateFormat sdm = new SimpleDateFormat("yyyyMMdd");
		String datePart = sdm.format(dt);
		return  datePart;
	}
}