/**
 * 
 */
package org.spider.topupservices.Engine;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.net.ssl.HttpsURLConnection;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.Logs.LogWriter;

/**
 * @author shaker
 *
 */
public class SmsSender {
	String destinationName;
	String clientId;
	String target;
	String LoadConf;
	String reply;
	String action;
	String userId;
	LogWriter logWriter;
	WeTopUpDS weTopUpDS;
	Configurations configurations;
	/**
	 * 
	 */
	public SmsSender(WeTopUpDS weTopUpDS, Configurations configurations, LogWriter logWriter) {
		this.destinationName="bubble.fyi";
		this.clientId="fees.school";
		this.target="ENGINE";
		this.LoadConf="N";
		this.reply="true";
		this.action="sendSMS";
		this.userId="115";	// bubble userID for we-top-up
		this.logWriter=logWriter;
		this.weTopUpDS=weTopUpDS;
		this.configurations=configurations;
//		this.logWriter.appendLog("SmsSender"+";action:"+this.action+";userId:"+this.userId);
	}
	/**
	 * 
	 * @param msisdn
	 * @param smsText
	 * @return 0 is success in sending to SMSC. Anything else is error.
	 * 
	 */
	public JsonEncoder sendSmsDirect(String msisdn, String smsText) {
		this.logWriter.appendAdditionalInfo(msisdn+":"+smsText);
		return sendMSGtoQueue(msisdn, smsText, true);
	}
	
	public JsonEncoder sendSms(String msisdn, String smsText) throws UnsupportedEncodingException{
		String errorCode = "-1";
		String errorMessage = "General error";
		String message = generateMessageString(msisdn,smsText);
		String urlReq = "https://10.10.1.12:8443/HttpReceiver/HttpReceiver?destinationName=bubble.fyi"
				+ "&destinationType=queue&clientid=bubble.fyi&target=ENGINE&LoadConf=N"
				+ "&reply=true&action=sendSMS";
		
		String data = message;
		byte[] postDataBytes = data.getBytes("UTF-8");
		
		try {
			HttpsTrustManage httptrustman = new HttpsTrustManage();
			httptrustman.TrustThyManager();
			URL url = new URL(urlReq);

			HttpURLConnection http = (HttpURLConnection) url.openConnection();
			http.setRequestProperty("Content-Type", "application/json; charset=utf8");
			http.setRequestMethod("POST");
			http.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length));
//			conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
			http.setDoOutput(true);
			http.getOutputStream().write(postDataBytes);
			http.getOutputStream().close();

			String res_status = http.getResponseMessage();
			BufferedReader in = new BufferedReader(new InputStreamReader(http.getInputStream()));
			LogWriter.LOGGER.info("sms sent successfully.");
			String inputLine;
			StringBuffer response1 = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				response1.append(inputLine);
			}
			in.close();

			System.out.println("SMSSender Response : " + response1.toString());
			if(response1.toString().startsWith("0")) {
				errorCode = "0";
				errorMessage = "SMS sent successfully.";
			}else {
				errorCode = "-5";
				errorMessage = "SMS sending failed.";
			}
		}catch(Exception e){
			errorCode = "10";
			errorMessage = "General Exception.";
			e.printStackTrace();
			LogWriter.LOGGER.severe(e.getMessage());
		}
		
		JsonEncoder je = new JsonEncoder();
		LogWriter.LOGGER.info("SMSSENDER errorCode : "+errorCode);
		je.addElement("ErrorCode", errorCode);
		je.addElement("ErrorMessage", errorMessage);
		
		je.buildJsonObject();
		return je;

	}
	
	private JsonEncoder sendMSGtoQueue(String msisdn, String smsText, boolean responseNeeded){
		String errorCode="-1";
		String errorMessage="Default error.";
		JsonEncoder je = new JsonEncoder();
		
		////    destinationName=bubble.fyi&destinationType=queue&clientid=bubble.fyi&target=ENGINE&LoadConf=N&message={"id":"18","msisdn":"88019XXXX","smsText":"this is first text from bubble!!!"}&reply=true&action=sendSMS
		Destination tmpQueue = null;
		try {
			InitialContext iniCtx = new InitialContext();
			Object tmp = iniCtx.lookup("ConnectionFactory");

			QueueConnectionFactory qcf = (QueueConnectionFactory) tmp;
			QueueConnection QueueConn = qcf.createQueueConnection();
			Queue queue = (Queue) iniCtx.lookup("java:/queue/" + this.destinationName); //java:/jms/queue/bubble.fyi java:/queue/bubble.fyi
			QueueSession qSession = QueueConn.createQueueSession(false,	QueueSession.AUTO_ACKNOWLEDGE);
			MessageProducer messageProducer = qSession.createProducer(queue);
			MapMessage newMsg = qSession.createMapMessage();
			MapMessage msg = generateMapMessage(msisdn, smsText, newMsg);
			//		boolean force = InhouseLogger.isTraceForced(msg);

			if (responseNeeded) {
				tmpQueue = qSession.createTemporaryQueue();
				msg.setJMSReplyTo(tmpQueue);
				LogWriter.LOGGER.info("SMS Temporary Queue is created");
			}
			QueueConn.start();

			messageProducer.send(msg);

			if (responseNeeded) {
				MessageConsumer messageConsumer = qSession.createConsumer(tmpQueue);

				msg = (MapMessage) messageConsumer.receive(30000);
				messageConsumer.close();
				if (msg != null) {
					String response=msg.getString("returnTxt");
					LogWriter.LOGGER.info("SendSMS:"+response);
					if(response.startsWith("0")) {
						errorCode="0";
						errorMessage="SMS sent successfully.";
						this.logWriter.appendLog("sendSms:"+errorCode);
						this.logWriter.setStatus(1);
					}else {
						errorCode="-2";
						errorMessage="SMS sending failed.";
						
						this.logWriter.appendLog("sendSms:"+errorCode);
						this.logWriter.setStatus(0);
					}
				} else {
					errorCode="-3";
					errorMessage="SMS sending failed : Timed out waiting for reply";
					LogWriter.LOGGER.severe("Timed out waiting for reply");
					this.logWriter.appendLog("sendSms:"+errorCode);
					this.logWriter.setStatus(0);
				}

			}else{
				//do nothing

			}
			// sessionTmp.close();
			messageProducer.close();
			qSession.close();

			QueueConn.stop();
			QueueConn.close();
			iniCtx.close();
		}catch(NamingException | JMSException ex){
			errorCode="-4";
			errorMessage="NamingException | JMSException";
			LogWriter.LOGGER.severe(ex.getStackTrace().toString());
			this.logWriter.appendLog("sendSmsJMS:"+errorCode);
			this.logWriter.setStatus(0);
		}catch(Exception e) {
			errorCode="10";
			errorMessage="General Exception.";
			LogWriter.LOGGER.severe(e.getStackTrace().toString());
			this.logWriter.appendLog("sendSms:"+errorCode);
			this.logWriter.setStatus(0);
		}
		
		je.addElement("ErrorCode", errorCode);
		je.addElement("ErrorMessage", errorMessage);
		je.buildJsonObject();
		
		return je;
	}

	public MapMessage generateMapMessage(String msisdn, String smsText,MapMessage msg) throws JMSException {
		String body="";
		msg.setString("body", body);
		////    destinationName=bubble.fyi&destinationType=queue&clientid=bubble.fyi&target=ENGINE&LoadConf=N&message={"id":"18","msisdn":"88019XXXX","smsText":"this is first text from bubble!!!"}&reply=true&action=sendSMS
		String message="{\"id\":\""+this.userId+"\",\"msisdn\":\""+msisdn+"\",\"smsText\":\""+smsText+"\"}";
		msg.setString("message", message);

		msg.setStringProperty("target",this.target);

		msg.setBoolean("traceON", false);

		msg.setString("destination", "httpService");
		msg.setString("action", this.action);
		msg.setString("clientid", this.clientId);
		msg.setString("reply", this.reply);
		msg.setString("LoadConf", this.LoadConf);
		return msg;
	}
	
	public String generateMessageString(String msisdn, String smsText) {
		////    destinationName=bubble.fyi&destinationType=queue&clientid=bubble.fyi&target=ENGINE&LoadConf=N&message={"id":"18","msisdn":"88019XXXX","smsText":"this is first text from bubble!!!"}&reply=true&action=sendSMS
		String message="{\"id\":\""+this.userId+"\",\"msisdn\":\""+msisdn+"\",\"smsText\":\""+smsText+"\"}";
		return message;
	}

}
