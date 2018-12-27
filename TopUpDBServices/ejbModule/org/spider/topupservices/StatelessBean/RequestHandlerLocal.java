package org.spider.topupservices.StatelessBean;

import javax.ejb.Local;
import javax.jms.JMSException;
import javax.jms.MapMessage;

import org.spider.topupservices.Initializations.Configurations;

/**
 * 
 * @author shaker
 *
 */
@Local
public interface RequestHandlerLocal {
	/**
	 * Abstract method to process new request.
	 * @param msg
	 * @param configurations
	 * @param force
	 * @return The result of the processing. String value.
	 * @throws JMSException
	 * @throws Exception
	 */
	public String processNewRequest(MapMessage msg, Configurations loadConf, boolean force) throws JMSException, Exception;	 
}
