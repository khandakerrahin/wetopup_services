package org.spider.topupservices.Engine;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.spider.topupservices.DataSources.WeTopUpDS;
import org.spider.topupservices.Logs.LogWriter;


public class QRequester {
	WeTopUpDS wetopupDS;
		String destinationName;
		String clientId;
		String target;
		String LoadConf;
		String reply;
		String action;
		String userId;
		LogWriter logWriter;
		/**
		 * 
		 */
		public QRequester(WeTopUpDS wetopupDS, LogWriter logWriter) {
			this.wetopupDS = wetopupDS;
			this.logWriter = logWriter;
			
			// LIVE
			this.destinationName="spidertopupdbcore";
			this.clientId="spidertopupdbcore";
			
			// SANDBOX
//			this.destinationName="spidertopupdbcoresandbox";
//			this.clientId="spidertopupdbcoresandbox";
			
			this.target="ENGINE";
			this.LoadConf="N";
			this.reply="true";
			this.action="";
		}

		/**
		 * 
		 * @param sender
		 * @param reciever
		 * @param subject
		 * @param emailbody
		 * @param responseNeeded
		 * @return
		 */
		public String sendRequesttoQueue(String body, String action, boolean responseNeeded){
			String retval="-1";

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
				MapMessage msg = generateMapMessage(body, action, newMsg);
				//		boolean force = InhouseLogger.isTraceForced(msg);

				if (responseNeeded) {
					tmpQueue = qSession.createTemporaryQueue();
					msg.setJMSReplyTo(tmpQueue);
					LogWriter.LOGGER.info("QRequester Temporary Queue is created");
				}
				QueueConn.start();

				messageProducer.send(msg);

				if (responseNeeded) {
					MessageConsumer messageConsumer = qSession.createConsumer(tmpQueue);

					msg = (MapMessage) messageConsumer.receive(30000);
					messageConsumer.close();
					if (msg != null) {
						String response=msg.getString("returnTxt");
						LogWriter.LOGGER.info("Qrequest:"+response);
						retval = response;
					} else {
						retval="{\"ErrorCode\":\"-3\", \"ErrorMessage\":\"Timed out waiting for reply\"}";
						LogWriter.LOGGER.severe("Timed out waiting for reply");
					//	this.logWriter.appendLog("SendEmail:"+retval);
						//this.logWriter.setStatus(0);
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
				retval="E";
				LogWriter.LOGGER.severe(ex.getStackTrace().toString());
				ex.printStackTrace();
			//	this.logWriter.appendLog("SendEmail:"+retval);
			//	this.logWriter.setStatus(0);
			}catch(Exception e) {
				retval="E";
				LogWriter.LOGGER.severe(e.getStackTrace().toString());
				e.printStackTrace();
			//	this.logWriter.appendLog("SendEmail:"+retval);
			//	this.logWriter.setStatus(0);
			}
			return retval;
		}
		
		public MapMessage generateMapMessage(String body,String action,MapMessage msg) throws JMSException {
			msg.setString("body", body);

			String message="";

			msg.setString("message", message);
			msg.setStringProperty("target",this.target);
			msg.setBoolean("traceON", false);
			msg.setString("destination", "httpService");
			msg.setString("action", action);
			msg.setString("clientid", this.clientId);
			msg.setString("reply", this.reply);
			msg.setString("LoadConf", this.LoadConf);
			return msg;
		}
}
