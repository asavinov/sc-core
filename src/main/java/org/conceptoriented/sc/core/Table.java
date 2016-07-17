package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Table {
	private Schema schema;
	public Schema getSchema() {
		return schema;
	}
	
	private final UUID id;
	public UUID getId() {
		return id;
	}

	private String name;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public boolean isPrimitive() {
		if(name.equalsIgnoreCase("Double") || name.equalsIgnoreCase("Integer") || name.equalsIgnoreCase("String")) return true;
		return false;
	}
	

	// 0,1,2,...,100,...,1000,...
	// [del)[clean)[new)
	// [rowRange) - all records that can be accessed
	
	public long maxRows = -1; // If more rows then they will be automatically deleted. In reality, there can be more rows if evaluate is not called but many records are pushed..  
	
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
		List<Column> columns = schema.getColumns(this.getName());

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
		
		// If too many records then mark some of them for deletion
		if(maxRows >= 0) {
			long excess = rowRange.getLength() - maxRows;
			if(excess > 0) {
				delRange.end += excess;
			}
		}
	}

	public void addNewRange() { // Mark dirty records as clean
		// Empty the old records range
		newRange.start = newRange.end;
	}

	public void removeDelRange() { // Really removed records marked for deletion by freeing the resources

		// Get all outgoing columns
		List<Column> columns = schema.getColumns(this.getName());

		for(Column column : columns) { // We must push a new value to all columns even if it has not been provided (null)
			// Remove initial elements of the column
			column.removeDelRange(delRange);
		}
		
		//
		// Update ranges.
		//

		// Empty the old records range
		delRange.start = delRange.end;
		rowRange.start = delRange.end;
	}

	public List<Record> read(Range range) {
		if(range == null) {
			range = this.getRowRange();
		}

		// Get all outgoing columns
		List<Column> columns = schema.getColumns(this.getName());

		List<Record> records = new ArrayList<Record>();
		for(long row = range.start; row < range.end; row++) {
			
			Record record = new Record();

			for(Column column : columns) {
				// Get value from the record
				Object value = column.getValue(row);
				// Store the value in the record
				record.set(column.getName(), value);
			}
			
			records.add(record);
		}
		
		return records;
	}

	public String toJson() {
		// Trick to avoid backslashing double quotes: use backticks and then replace it at the end 
		String jid = "`id`: `" + this.getId() + "`";
		String jname = "`name`: `" + this.getName() + "`";
		
		String json = jid + ", " + jname;

		return ("{" + json + "}").replace('`', '"');
	}
	
	@Override
	public String toString() {
		return "[" + name + "]";
	}
	
	@Override
	public boolean equals(Object aThat) {
		if (this == aThat) return true;
		if ( !(aThat instanceof Table) ) return false;
		
		Table that = (Table)aThat;
		
		if(!that.getId().toString().equals(id.toString())) return false;
		
		return true;
	}

	 public Table(Schema schema, String name) {
		this.schema = schema;
		this.id = UUID.randomUUID();
		this.name = name;
	}

}