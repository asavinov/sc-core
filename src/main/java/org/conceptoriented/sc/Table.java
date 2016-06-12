package org.conceptoriented.sc;

import java.util.List;

public class Table {
	Space space;

	String name;
	
	
	
	// We need to store dirty interval. These are supposed to be new records.
	public Range added;
	
	// We need to store start and end rows
	public Range rows;

	// We need to store records marked for removal after they have been used for evaluating new records.
	public Range removed;

	public void push(Record record) {

		// It will append the record to all columns
		List<Column> columns = null;
		for(Column column : columns) {

			// Get value from the record
			Object value = record.get(column.getName());
			
			// Append the value to the column
			column.push(value);
		}
		
		// Mark this record as dirty by adding it to the range of new records
	}

	public Table(Space space, String name) {
		this.space = space;
		this.name = name;
	}

}
