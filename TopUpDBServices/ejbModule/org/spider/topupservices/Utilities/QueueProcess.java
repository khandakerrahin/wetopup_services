/**
 * 
 */
package org.spider.topupservices.Utilities;

import java.util.logging.Logger;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * @author shaker
 *
 */
public class QueueProcess {
	
	/**
	 * 
	 */
	private static final Logger LOGGER = Logger.getLogger(QueueProcess.class.getName());
	public QueueProcess() {
		// TODO Auto-generated constructor stub
	}
	public static void Send(MapMessage msg, Message message, String reply) throws JMSException, NamingException{
		
		MessageProducer messageProducer = null;
		QueueSession session =null;
		QueueConnection connection = null;
		InitialContext ic=null;
		try {
			if(message.getJMSReplyTo() != null) {
				ic=new InitialContext();
				Object tmp = ic.lookup("ConnectionFactory");
				QueueConnectionFactory qcf = (QueueConnectionFactory) tmp;
				connection = qcf.createQueueConnection();
				session = connection.createQueueSession(false,QueueSession.AUTO_ACKNOWLEDGE);
				Message response = session.createMessage();
				response.setJMSCorrelationID(message.getJMSCorrelationID());
				messageProducer = session.createSender((Queue) message.getJMSReplyTo());
				
				MapMessage m = session.createMapMessage();
				m.setString("returnTxt",""+reply);
				messageProducer.send(m);
				messageProducer.close();
				session.close();
				connection.stop();
				connection.close();
				ic.close();
			}
		}catch (JMSException|NamingException e) {
//			e.printStackTrace();
			LOGGER.severe(e.getMessage());
			throw e;
		}
	}
}
