package org.conceptoriented.sc.core;

import java.util.HashMap;
import java.util.Map;

public class Record {
	
	Map<String, Object> recod = new HashMap<String, Object>();

	public Object get(String name) {
		return recod.get(name);
	}

	public void set(String name, Object value) {
		recod.put(name, value);
	}
	
	public Record() {
	}

}
