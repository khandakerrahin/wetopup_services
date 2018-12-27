/**
 * 
 */
package org.spider.topupservices.Utilities;

import java.security.SecureRandom;

/**
 * @author shaker
 *
 */
public class RandomStringGenerator {

	/**
	 * 
	 * @param characterSet Character set to use. Eg. "1234567890ABEDEFGHIJKLMNOPQRSTUVWXYZ"
	 * @param length Length of output random string
	 * @return a random string of size <b>length<b>
	 */
	public static String getRandomString(String characterSet, int length ){
		SecureRandom rnd = new SecureRandom();
		StringBuilder sb = new StringBuilder( length );
		for( int i = 0; i < length; i++ ) 
			sb.append( characterSet.charAt( rnd.nextInt(characterSet.length()) ) );
		return sb.toString();
	}

}
