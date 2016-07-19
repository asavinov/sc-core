package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;

public class QName {
	
	public List<String> names = new ArrayList<String>();
	
	public Column resolveColumn(Schema schema, Table table) { // Table is used only if no table name is available
		
		String tableName = getTableName();
		if(tableName == null) {
			if(table != null)
				tableName = table.getName();
			else
				return null;
		}
		
		Column column = null;
		if(getColumnName() != null) {
			column = schema.getColumn(tableName, getColumnName());
		}
		
		return column;
	}

	public String getTableName() {
		if(names.size() < 2) return null;
		return names.get(names.size()-2);
	}

	public String getColumnName() {
		if(names.size() < 1) return null;
		return names.get(names.size()-1); // Last segment
	}

}
