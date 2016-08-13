package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 *
 */
public class Record {
	
	// Alternative: Apache Commons CaseInsensitiveMap 
	Map<String, Object> recod = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);

	public Object get(String name) {
		return recod.get(name);
	}

	public void set(String name, Object value) {
		recod.put(name, value);
	}
	
	public String toJson() {
		// Loop over all keys
		String data = "";
		for (Map.Entry<String, Object> entry : recod.entrySet())
		{
			String data_elem = "`" + entry.getKey() + "`:`" + entry.getValue() + "`, ";
			data += data_elem;
		}		
		if(data.length() > 2) {
			data = data.substring(0, data.length()-2);
		}
		
		return ("{" + data + "}").replace('`', '"'); // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
	}
	public String toCsv(List<String> columns) { // Comma separated
		String data = "";
		// Loop over all columns
		for(String column : columns) {
			Object value = this.get(column);
			String data_elem = "`" + value.toString() + "`,";
			data += data_elem;
		}
		if(data.length() > 1) {
			data = data.substring(0, data.length()-1);
		}
		
		return data.replace('`', '"'); // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
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

	public static List<Record> fromJsonList(String jsonString) {
		List<Record> records = new ArrayList<Record>();

		Object token = new JSONTokener(jsonString).nextValue();
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
	
	public static List<Record> fromCsvList(String csvString) {
		List<Record> records = new ArrayList<Record>();
		
		List<String> lines = new ArrayList<String>(Arrays.asList(csvString.split("\\r?\\n")));

		// Create a list of column names
		String headerLine = lines.get(0);
		List<String> columns = new ArrayList<String>(Arrays.asList(headerLine.split(",")));
		for(int i=0; i < columns.size(); i++) { 
			columns.set(i, columns.get(i).trim());
		}

		// Loop over all lines
		for (int i=1; i < lines.size(); i++) {
			String line = lines.get(i);
			if(line == null || line.trim().isEmpty()) continue;
			
			Record record = new Record();

			String[] fields = line.split(",");
			for(int j=0; j<fields.length; j++) {
				if(j >= columns.size()) break; // More field values than columns
				record.set(columns.get(j), fields[j]);
			}
			records.add(record);
		}

		return records;
	}

	public Record() {
	}

}
