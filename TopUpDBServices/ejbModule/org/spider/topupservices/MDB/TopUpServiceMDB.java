package org.spider.topupservices.MDB;

import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.EJB;
import javax.ejb.MessageDriven;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.naming.NamingException;

import org.spider.topupservices.Initializations.Configurations;
import org.spider.topupservices.StatelessBean.RequestHandler;
import org.spider.topupservices.StatelessBean.RequestHandlerLocal;
import org.spider.topupservices.Utilities.QueueProcess;

/**
 * Message-Driven Bean implementation class for: spidertopup
 */

@MessageDriven(
		activationConfig = { 
				@ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/spidertopup"), 
				@ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
				@ActivationConfigProperty(propertyName = "subscriptionDurability", propertyValue = "Durable"),
				@ActivationConfigProperty(propertyName = "clientID", propertyValue = "spidertopup"),
				@ActivationConfigProperty(propertyName = "maxSession", propertyValue = "120"),
				@ActivationConfigProperty(propertyName = "messageSelector", propertyValue = "target = 'ENGINE'")
		}, 
		//mappedName = "java:/queue/testQ")
//		mappedName = "java:/jms/queue/spidertopup")
		mappedName = "java:/queue/spidertopup")


public class TopUpServiceMDB implements MessageListener {
	@EJB(lookup="java:module/RequestHandler")
	private RequestHandlerLocal reqHandle;
	QueueConnection connection;
	QueueSession session;
	MessageProducer messageProducer;
	String appName  = "spidertopup";
	public Configurations loadConf = new Configurations();
	private static final Logger LOGGER = Logger.getLogger(TopUpServiceMDB.class.getName());
	//    AlarmProducer dbAlarm     = new AlarmProducer(4,"database connection fails",appName);
	//    AlarmProducer jmsAlarm    = new AlarmProducer(3,"JMS Exception",appName);
	//    AlarmProducer namingAlarm = new AlarmProducer(5,"Naming Exception",appName);
	/**
	 * Default constructor. 
	 */
	public TopUpServiceMDB() {
		// TODO Auto-generated constructor stub
	}

	@PostConstruct
	public void loadConfiguration() {
		try {
			LOGGER.info(appName+" MDB loaded");

			LOGGER.info("Loading configuration from DB ..");
			loadConf.loadConfigurationFromDB();

			LOGGER.info("Loading configuration from DB complete.");

		} catch (Exception ex) {
			LOGGER.severe(ex.getMessage());
		}
	}

	@PreDestroy
	public void PreDestroy() {
		LOGGER.info(appName+" MDB destroyed");
	}
	/**
	 * @see MessageListener#onMessage(Message)
	 */
	public void onMessage(Message message) {
		if (message instanceof MapMessage) {
			MapMessage msg = (MapMessage) message;
			//msg.
			try {
				boolean force = true;//InhouseLogger.isTraceForced(msg);            
				LOGGER.info("Received Message at "+appName+"=" + msg.getString("message"));

				String loadConfigurations = msg.getString("LoadConf");
				//boolean isNull = NullPointExceptionHendler.isNullOrEmptyAfterTrim(loadConfigurations);
				String reply = "";
				if(/*!isNull &&*/ loadConfigurations.equals("Y")){
					loadConf.loadConfigurationFromDB();
					reply ="Configurations Loaded";
				}else{               
					reply  =  reqHandle.processNewRequest(msg,loadConf,force);
					//LOGGER.info("Reply: "+reply);
				}
				// reply  =  reqHandle.processNewRequest(msg,configurations,force);

				if (message.getJMSReplyTo() != null) { // Reply to the temporary queue

					try {
						QueueProcess.Send(msg, message, reply+"");                    	 
						//jmsAlarm.clearAlarm(); dbAlarm.clearAlarm();   
					} catch (NamingException e) {
						//jmsAlarm.setAlarm(); 
						LOGGER.severe("exception in responseQueue:" + e.getMessage());
					}
				}
				//jmsAlarm.clearAlarm();dbAlarm.clearAlarm();namingAlarm.clearAlarm();
			} catch (JMSException e) {            
				e.printStackTrace();
				LOGGER.severe(e.getMessage());
				//jmsAlarm.setAlarm();
				try {
					if (message.getJMSReplyTo() != null) {					    
						QueueProcess.Send(msg, message, "F");
						//jmsAlarm.clearAlarm();
						//dbAlarm.clearAlarm();  
					}
				} catch (NamingException | JMSException ex ) {
					//jmsAlarm.setAlarm();	
					LOGGER.severe("exception in "+appName+":"+ex.getMessage());
				} 
			} catch (Exception e1) {
				e1.printStackTrace();
				try {
					if (message.getJMSReplyTo() != null) {					    
						QueueProcess.Send(msg, message, "F");
						//jmsAlarm.clearAlarm();
						//dbAlarm.clearAlarm();  
					}
				} catch (NamingException | JMSException ex ) {
					//jmsAlarm.setAlarm();	
					LOGGER.severe("exception in "+appName+":"+ex.getMessage());
				} 
			}
		} else {
			LOGGER.warning("INVALID MESSAGE RECEIVED");
		}
	}

}
