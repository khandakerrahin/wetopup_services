/**
 * 
 */
package org.spider.topupservices.Engine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.spider.topupservices.Logs.LogWriter;
import org.spider.topupservices.Utilities.NullPointerExceptionHandler;

import java.util.Map;

/**
 * @author hafiz
 *
 */
public class JsonDecoder {
	JsonObject jsonObject;
	String inputJSONString;
	Map<String,JsonValue> jsonObjectMap;
	String errorCode;
	String errorMessage;
//	private static final Logger LOGGER = LogWriter.LOGGER.getLogger(JsonDecoder.class.getName());
	/**
	 * 
	 */
	public JsonDecoder(String jsonString) {
		this.errorCode="-1";
		this.inputJSONString=jsonString;
		InputStream is;
		jsonObject=null;
		jsonObjectMap=null;
		try {
			//LogWriter.LOGGER.info("Decoding : "+ this.inputJSONString);
			
			if(!jsonString.isEmpty()) {

				is = new ByteArrayInputStream(jsonString.getBytes("UTF-8")); //jsonString.getBytes("UTF-8")
				JsonReader jsonReader = Json.createReader(is);
				jsonObject = jsonReader.readObject();
				jsonObjectMap=jsonObject;
				errorCode="0";
				jsonReader.close();
				is.close();
			}else {
				errorCode="1"; this.errorMessage="Empty string supplied.";
			}
		} catch (UnsupportedEncodingException e) {
			errorCode="2"; this.errorMessage=e.getMessage();

			LogWriter.LOGGER.info("Decoding ERROR: UnsupportedEncodingException " +e);
		} catch (IOException e) {
			errorCode="3"; this.errorMessage=e.getMessage();
			LogWriter.LOGGER.info("Decoding ERROR: 1  "+e);
			e.printStackTrace();
		} catch (Exception e) {
			errorCode="4"; this.errorMessage=e.getMessage();
			LogWriter.LOGGER.info("Decoding ERROR: 2 "+e);e.printStackTrace();
		}
	}
	/**
	 * @return the jsonObject
	 */
	public JsonObject getJsonObject() {
		return jsonObject;
	}
	/**
	 * @return the inputJSONString
	 */
	public String getInputJSONString() {
		return inputJSONString;
	}
	/**
	 * @return the jsonObjectMap
	 */
	public Map<String, JsonValue> getJsonObjectMap() {
		return jsonObjectMap;
	}
	/**
	 * @return the errorCode
	 */
	public String getErrorCode() {
		return errorCode;
	}
	/**
	 * @return the errorMessage
	 */
	public String getErrorMessage() {
		return errorMessage;
	}
	public String getEString(String s) {
		try{
			return this.jsonObject.getString(s);
		}catch(NullPointerException n) {
			return "";
		}catch(ClassCastException cE) {
			return "";
		}
	}
	public int getEInt(String s) {
		try{
			return this.jsonObject.getInt(s);
		}catch(NullPointerException n) {
			return 0;
		}catch(ClassCastException cE) {
			return 0;
		}catch(Exception e) {
			return 0;
		}
	}
	public String getIntString(String s) {
		try{
			return this.jsonObject.getString(s);
		}catch(NullPointerException n) {
			return "0";
		}catch(Exception e) {
			return "0";
		}
	}
	/**
	 * 
	 * @param s
	 * @return null if NullPointerException
	 */
	public String getNString(String s) {
		try{
			return this.jsonObject.getString(s);
		}catch(NullPointerException n) {
			return "";
		}catch(ClassCastException cE) {
			return null;
		}
	}
	/**
	 * 
	 * @param string
	 * @return true if available, otherwise false
	 */
	public boolean isParameterPresent(String string) {
		return this.jsonObject.containsKey(string);
	}
}
