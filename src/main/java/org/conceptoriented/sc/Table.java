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
	

	// 0,1,2,...,100,...,1000,...
	// [del)[clean)[new)
	// [rowRange) - all records that can be accessed
	
	// Records to be deleted after the next re-evaluation
	// We need to store records marked for removal after they have been used for evaluating new records.
	protected Range delRange = new Range();
	public Range getDelRange() {
		return new Range(delRange);
	}

	// These records have been already evaluated (clean)
	// We need to store start and end rows
	protected Range rowRange = new Range();
	public Range getRowRange() {
		return new Range(rowRange);
	}

	// Records added but not evaluated yet (dirty). They are supposed to be evaluated in the next iteration. 
	// We need to store dirty interval. These are supposed to be new records.
	protected Range newRange = new Range();
	public Range getNewRange() {
		return new Range(newRange);
	}
	
	public void push(Record record) {

		// Get all outgoing columns
		List<Column> columns = space.getColumns(this.getName());

		for(Column column : columns) { // We must push a new value to all columns even if it has not been provided (null)

			// Get value from the record
			Object value = record.get(column.getName());
			
			// Append the value to the column (even if it is null)
			column.push(value);
		}
		
		//
		// Update ranges.
		//

		// Mark this record as dirty by adding it to the range of new records
		newRange.end++;
		rowRange.end++;
		
		// Some record might need to be marked for deletion (if the table gets too long)
	}

	public void addNewRange() { // Mark dirty records as clean
		// Empty the old records range
		newRange.start = newRange.end;
	}

	public void removeDelRange() { // Really removed records marked for deletion by freeing the resources

		// Get all outgoing columns
		List<Column> columns = space.getColumns(this.getName());

		for(Column column : columns) { // We must push a new value to all columns even if it has not been provided (null)
			// Remove initial elements of the column
			column.removeDelRange(this.getDelRange());
		}
		
		//
		// Update ranges.
		//

		// Empty the old records range
		delRange.start = delRange.end;
		rowRange.start = delRange.end;
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
