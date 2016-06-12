package org.conceptoriented.sc;

import java.util.Map;

public class Record {
	
	Map<String, Object> recod;

	public Object get(String name) {
		return recod.get(name);
	}

	public void set(String name, Object value) {
		recod.put(name, value);
	}

}
