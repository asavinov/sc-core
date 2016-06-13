package org.conceptoriented.sc;

import java.util.List;

public class Table {
	Space space;

	String name;
	public String getName() {
		return name;
	}
	
	public boolean isPrimitive() {
		if(name.equalsIgnoreCase("Double") || name.equalsIgnoreCase("Integer") || name.equalsIgnoreCase("String")) return true;
		return false;
	}
	
	// We need to store dirty interval. These are supposed to be new records.
	public Range added;
	
	// We need to store start and end rows
	public Range rows;

	// We need to store records marked for removal after they have been used for evaluating new records.
	public Range removed;

	public void push(Record record) {

		// Get all outgoing columns
		List<Column> columns = space.getColumns(this.getName());

		for(Column column : columns) {

			// Get value from the record
			Object value = record.get(column.getName());
			
			// Append the value to the column (even if it is null)
			column.push(value);
		}
		
		// Mark this record as dirty by adding it to the range of new records
	}

	@Override
	public String toString() {
		return "[" + name + "]";
	}
	
	public Table(Space space, String name) {
		this.space = space;
		this.name = name;
	}

}
