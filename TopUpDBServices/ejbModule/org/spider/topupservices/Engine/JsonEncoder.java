/**
 * 
 */
package org.spider.topupservices.Engine;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * @author hafiz
 *
 */
public class JsonEncoder {
	JsonObjectBuilder jsonObjectBuilder;
	JsonObject jsonObject;
	/**
	 * Primary constructor
	 */
	public JsonEncoder() {
		jsonObjectBuilder = Json.createObjectBuilder();
	}
	
	public JsonObject buildJsonObject() {
		jsonObject = jsonObjectBuilder.build();
		return jsonObject;
	}
	/**
	 * Preferably use
	 * this.getJsonObjectBuilder().add(key, value);
	 * @param key
	 * @param value
	 */
	public void addElement(String key,String value) {
		jsonObjectBuilder.add(key, value);
	}
	public void removeElement(String key) {
		this.getJsonObject().remove(key);
	}
	/**
	 * Used to add elements
	 * @return the jsonObjectBuilder
	 */
	public JsonObjectBuilder getJsonObjectBuilder() {
		return jsonObjectBuilder;
	}
	/**
	 * @return the jsonObject
	 */
	public JsonObject getJsonObject() {
		return jsonObject;
	}
}
