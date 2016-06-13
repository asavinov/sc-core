package org.conceptoriented.sc;

import java.util.ArrayList;
import java.util.List;

/**
 * Stream space stores the complete data state and is able to consistently update it. 
 */
public class Space {
	
	public String name;
	
	// A list of all input/output streams

	//
	// Tables
	//
	List<Table> tables = new ArrayList<Table>();
	public Table createTable(String name) {
		Table table = new Table(this, name);
		tables.add(table);
		return table;
	}
	public Table getTable(String table) {
		for(Table tab : tables) {
			if(tab.getName() == table) return tab;
		}
		return null;
	}

	//
	// Columns
	//
	List<Column> columns = new ArrayList<Column>();
	public Column createColumn(String name, String input, String output) {
		Column column = new Column(this, name, input, output);
		columns.add(column);
		return column;
	}
	public Column getColumn(String table, String column) {
		for(Column col : columns) {
			if(col.getName() == column) return col;
		}
		return null;
	}
	public List<Column> getColumns(String table) {
		List<Column> res = new ArrayList<Column>();
		for(Column col : columns) {
			if(col.input.getName() == table) {
				res.add(col);
			}
		}
		return res;
	}

	//
	// Data (state)
	//
	
	
	// One approach is not to separate input into a dedicated operation (reading from input stream)
	// Instead, we encode input (reading from streams and inserting into tables) as normal column evaluation.
	// A column which leads from input streams to normal tables has a special type. 
	// Also, it is a starting point in column dependency graph. 
	// For example, such a column like any other column has to iterate through all input record and produce output records. 
	// The difference is that input records are removed immediately after reading but it is a specific function of the input stream. 
	// Also, input streams do not have normal column structure because they store row objects. We can think of them as having one column storing arbitrary objects (records).
	// Maybe, we can provide special column types used for feeding the streams which are configured using mapping/transformation rules rather than using custom code.
	// Such columns know how to read input records and how to write them. Also, input/output streams are extensions of normal tables which could be configured in some special way.
	// It would be also interesting to have a possibility to use several import columns leading from one input stream and several export columns leading to one output stream. 
	
	// Major steps
	// - create schema objects either programmatically or by de-serializing from JSON:
	// - configure schema objects. Importantly, every column has to get its evaluator instance: instance of plug-in, instance of COEL expression, instance of Mapping specification etc.
	//   - normal columns either explicitly assigned a plug-in or find this plug-in by name convention. 
	//   - import columns need to be configured my using Mapping configuration so that their evaluate function can pop records from input stream and push then into the type table.
	//     it also can use a (simple) filter so that not all input records are imported. A mapping is simply an output record with fields defined in terms of the input column values (COEL TUPLE) but it can be more complex in the case of complex import object structure (JSON, XML, audio etc.).
	//     Import mapping specification can be executed in terms of the specific Record implementation which it exposes (rather than column structure). 
	//   - export columns are normal user-defined columns which return a record and push it to the type table (which can be export table)
	//   - export columns can be a special (export) column which accepts mapping as its formula. for example, it will read certain columns and then push a record to the output.
	//     the only problem is that we do not want to output all records so we need a mechanism of filtering for mapping
	//     filter can be specified as some other binary column (so it depends on it) computed by the user: if it is true then this export column outputs a record according to the mapping
	//
	// - evaluate space
	//   - retrieve dependencies from all column functions
	//   - build dependency graph
	//   - initialize column functions by passing column references and other parameters needed for evaluation.
	//   - execute all column evaluations.
	//     - a column might not need to be evaluated if it is not dirty (it is determined either by the driver or the column evaluator itself)
	//     - import columns determine their dirty status from the import table (if its has new record)
	//     - evaluation of import columns will add new records to normal tables by making them dirty
	//     - normal columns can also append records to other tables by making them dirty
	//     - records can be also appended to export tables but here nothing happens (the export thread will consume them)
	//       we never write to export explicitly - we need to return the corresponding object to be pushed. 

	public void evaluate() {
		
		//
		// Evaluate the space. Make again consistent (non-dirty).
		// Bring the state back to consistent state by re-computing the values which are known to be dirty.
		//

		// Build a list/graph of columns to be evaluated. Either in the sequence of creation, or using dependencies.
		// Evaluate each column individually from this structure. 
		// Any column has to provide an evaluation function which knows how to compute the output value.
		List<Column> columns = this.columns;

		// For each dirty value, evaluate it again and store the result
		for(Column column : columns) {
			if(column.evaluator == null) continue;
			column.evaluate();
		}
		
		
		// How do we treat tables?
		// We need to take into account that records might have been added - so only they have to be evaluated
		// We need to take into account that some tables can appended during other column evaluation. 
		// We need to provide a mechanism for marking records for deletion
		// We need to provide a mechanism for real deletion of records: either explicitly by the driver or implicitly by the table data access operator, e.g., if this record is read 
		
	}
	
	public Space(String name) {
		this.name = name;
		
		// Create primitive tables
		Table doubleType = createTable("Double");
		tables.add(doubleType);
		
		Table integerType = createTable("Integer");
		tables.add(integerType);
		
		Table stringType = createTable("String");
		tables.add(stringType);
	}

}
