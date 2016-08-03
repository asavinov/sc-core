package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A dot separated sequence of names. 
 * The sequence can start either from schema or from table or from column. 
 * The sequence can continue as a column path. 
 */
public class QName {
	
	public List<String> names = new ArrayList<String>();
	
	// Assume that it is a column path, resolve all individual columns relative to the specified table
	// The returned array contains as many elements as the names in the sequence
	public List<Column> resolveColumns(Table table) {
		List<Column> result = new ArrayList<Column>();
		
		Schema schema = table.getSchema();
		
		for(String name : names) {
			Column column = schema.getColumn(table.getName(), name);
			result.add(column);
			table = column.getOutput();
		}
		
		return result;
	}

	// Assume that the sequence is a fully qualified name of a column
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
