package org.conceptoriented.sc.core;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
	
	//
	// Rules for automatic population and de-population (similar to auto-evaluation)
	//

	// Max age. Old records will be automatically deleted. 0 means immediate deletion of new records. MAX, NULL or -1 mean any age and hence no auto-deletion.  
	protected long maxAge = -1;  

	// If more rows are added then then the oldest will be marked for deletion.  
	// The real deletion happens only after evaluation.
	protected long maxLength = -1;  
	public long getMaxLength() {
		return maxLength;
	}
	public void setMaxLength(long maxLength) {
		
		if(maxLength == this.maxLength) {
			return;
		}
		if(maxLength < 0) {
			// Remove all del-markers. Any length is possible.
		}
		else if(maxLength > this.maxLength) {
			// Remove some del-markers until fit into new max length
		}
		else {
			// Add some del-markers to fit into the new max length
		}

		this.maxLength = maxLength;
	}

	//
	// Population dirty state.
	//

	// 0,1,2,...,100,...,1000,...
	// [del)[clean)[new)
	// [rowRange) - all records that physically exist and can be accessed
	// |clean|+|new| <= maxLength
	// New records have been physically added but this change of data state was not propagated
	// Deleted records were marked for deletion but before physical deletion this change of state has to be propagated
	// Clean records are records the addition of which has been already propagated through the schema
	
	// Records to be deleted after the next re-evaluation
	// We need to store records marked for removal after they have been used for evaluating new records.
	protected Range delRange = new Range();
	public Range getDelRange() {
		return new Range(delRange);
	}
	public Range getNonDelRange() {
		return new Range(getCleanRange().start, getNewRange().end);
	}

	// These records have been already evaluated (clean)
	// We need to store start and end rows
	protected Range cleanRange = new Range();
	public Range getCleanRange() {
		return new Range(cleanRange);
	}

	// Records added but not evaluated yet (dirty). They are supposed to be evaluated in the next iteration. 
	// We need to store dirty interval. These are supposed to be new records.
	protected Range newRange = new Range();
	public Range getNewRange() {
		return new Range(newRange);
	}
	

	public void markCleanAsNew() { // Mark clean records as dirty (new). Deleted range does not change.
		// [del)[clean)[new)
		cleanRange.end = cleanRange.start; // No clean records
		newRange.start = cleanRange.start; // All new range
	}

	public void markNewAsClean() { // Mark dirty records as clean
		// [del)[clean)[new)
		cleanRange.end = newRange.end; // All clean records
		newRange.start = newRange.end; // No new range
	}

	public void markAllAsDel() { // Mark all records as deleted
		// [del)[clean)[new)

		delRange.end = newRange.end;
		
		cleanRange.start = newRange.end;
		cleanRange.end = newRange.end;
		
		newRange.start = newRange.end;
		newRange.end = newRange.end;
	}

	//
	// Operations with records
	//

	Instant appendTime = Instant.now(); // Last time a record was (successfully) appended. It is equal to the time stamp of the last record.
	public void setAppendTime() {
		this.appendTime = Instant.now();
	}
	public Duration durationFromLastAppend() {
		return Duration.between(this.appendTime, Instant.now());
	}

	public void append(Record record) {

		// Get all outgoing columns
		List<Column> columns = this.schema.getColumns(this.getName());

		for(Column column : columns) { // We must append a new value to all columns even if it has not been provided (null)

			// Get value from the record
			Object value = record.get(column.getName());
			
			// Append the value to the column (even if it is null)
			column.appendValue(value);
		}
		
		//
		// Update ranges.
		//

		// Mark this record as dirty by adding it to the range of new records
		newRange.end++;
		
		// If too many records then mark some of them (in the beginning) for deletion
		if(maxLength >= 0) {
			long excess = (cleanRange.getLength() + newRange.getLength()) - maxLength;
			if(excess > 0) {
				delRange.end += excess;
				
				cleanRange.start = delRange.end;
				if(cleanRange.end < cleanRange.start) {
					cleanRange.end = cleanRange.start;
					newRange.start = cleanRange.end;
				}
			}
		}
		
		setAppendTime(); // Store the time of append operation
	}
	public void append(List<Record> records, Map<String, String> columnMapping) {
		for(Record record : records) {
			this.append(record);
		}
	}

	public long find(Record record, boolean append) {

		List<String> names = record.getNames();
		List<Object> values = names.stream().map(x -> record.get(x)).collect(Collectors.<Object>toList());
		List<Column> columns = names.stream().map(x -> this.getSchema().getColumn(this.getName(), x)).collect(Collectors.<Column>toList());
		
		Range range = getNonDelRange();
		long index = -1;
		for(long i=range.start; i<range.end; i++) { // Scan all records and compare

			boolean found = true;
			for(int j=0; j<names.size(); j++) {
				// TODO: The same number in Double and Integer will not be equal. So we need cast to some common type at some level of the system or here.
				Object recordValue = values.get(j);
				Object columnValue = columns.get(j).getValue(i);

				// Compare two values of different types
				if(recordValue instanceof Number && columnValue instanceof Number) {
					if( ((Number) recordValue).doubleValue() != ((Number) columnValue).doubleValue() ) { found = false; break; }
					// OLD: if(!recordValue.equals(columnValue)) { found = false; break; }
				}
				else {
					// Compare nullable objects
					if( !com.google.common.base.Objects.equal(recordValue, columnValue) ) { found = false; break; }
				}
			}
			
			if(found) {
				index = i;
				break;
			}
		}
		
		if(append && index < 0) {
			append(record);
			index = getNewRange().end - 1;
		}

		return index;
	}

	public void remove() {
		this.markAllAsDel();
		this.removeDelRange();
	}

	public void removeDelRange() { // Physically remove records marked for deletion by freeing the resources

		// Get all outgoing columns
		List<Column> columns = schema.getColumns(this.getName());

		for(Column column : columns) { // We must append a new value to all columns even if it has not been provided (null)
			// Remove initial elements of the column
			column.removeDelRange(delRange);
		}
		
		//
		// Update ranges.
		//

		// Empty the old records range
		delRange.start = delRange.end;
		cleanRange.start = delRange.end;
	}

	public List<Record> read(Range range) {
		if(range == null) {
			range = getNonDelRange();
		}

		// Get all outgoing columns
		List<Column> columns = schema.getColumns(this.getName());

		List<Record> records = new ArrayList<Record>();
		for(long row = range.start; row < range.end; row++) {
			
			Record record = new Record();
			record.set("_id", row);

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

	//
	// Serialization and construction
	//

	public String toJson() {
		// Trick to avoid backslashing double quotes: use backticks and then replace it at the end 
		String jid = "`id`: `" + this.getId() + "`";
		String jname = "`name`: `" + this.getName() + "`";
		
		String jmaxLength = "`maxLength`: " + this.getMaxLength() + "";

		String json = jid + ", " + jname + ", " + jmaxLength;

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
