package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class Record {
	
	Map<String, Object> recod = new HashMap<String, Object>();

	public Object get(String name) {
		return recod.get(name);
	}

	public void set(String name, Object value) {
		recod.put(name, value);
	}
	
	public String toJson() {
		// Loop over all keys
		String json = "";
		for (Map.Entry<String, Object> entry : recod.entrySet())
		{
			String jcol = "`" + entry.getKey() + "`:`" + entry.getValue() + "`, ";
			json += jcol;
		}		
		if(json.length() > 2) {
			json = json.substring(0, json.length()-2);
		}
		
		return ("{" + json + "}").replace('`', '"'); // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
	}
	
	public static Record fromJson(String json) {
		JSONObject obj = new JSONObject(json);
		return fromJsonObject(obj);
	}

	public static Record fromJsonObject(JSONObject obj) {
		Record record = new Record();
		
		Iterator<?> keys = obj.keys();
		while(keys.hasNext()) {
		    String key = (String)keys.next(); // Column name
		    Object value = obj.get(key);
		    record.set(key, value);
		}
		return record;
	}

	public static List<Record> fromJsonList(String json) {
		List<Record> records = new ArrayList<Record>();

		Object token = new JSONTokener(json).nextValue();
		JSONArray arr;
		if (token instanceof JSONArray) { // Array of records
			arr = (JSONArray) token;
		}
		else { // (token instanceof JSONObject)
			if(!((JSONObject)token).has("data")) { // token is Record object
				records.add(Record.fromJsonObject((JSONObject)token));
				return records;
			}
			else {
				Object content = ((JSONObject)token).get("data");
				if(content instanceof JSONObject) { // content is Record object
					records.add(Record.fromJsonObject((JSONObject)content));
					return records;
				}
				else if(content instanceof JSONArray) {
					arr = (JSONArray) content;
				}
				else {
					return null;
				}
			}

		} 
		
		// Loop over all list
		for (int i = 0 ; i < arr.length(); i++) {
			JSONObject jrec = arr.getJSONObject(i);
			Record record = Record.fromJsonObject(jrec);
			records.add(record);
		}

		return records;
	}
	
	public Record() {
	}

}
