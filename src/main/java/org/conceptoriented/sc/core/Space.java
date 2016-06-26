package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Stream space stores the complete data state and is able to consistently update it. 
 */
public class Space {
	
	private final UUID id;
	public UUID getId() {
		return id;
	}

	private String name;
	public String getName() {
		return name;
	}
	
	// A list of all input/output streams

	//
	// Tables
	//
	private List<Table> tables = new ArrayList<Table>();
	public List<Table> getTables() {
		return tables;
	}
	public Table getTable(String table) {
		for(Table tab : tables) {
			if(tab.getName() == table) return tab;
		}
		return null;
	}
	public Table createTable(String name) {
		Table table = new Table(this, name);
		tables.add(table);
		return table;
	}

	//
	// Columns
	//
	private List<Column> columns = new ArrayList<Column>();
	public List<Column> getColumns() {
		return columns;
	}
	public List<Column> getColumns(String table) {
		List<Column> res = new ArrayList<Column>();
		for(Column col : columns) {
			if(col.getInput().getName() == table) {
				res.add(col);
			}
		}
		return res;
	}
	public Column getColumn(String table, String column) {
		for(Column col : columns) {
			if(col.getName() == column) return col;
		}
		return null;
	}
	public Column createColumn(String name, String input, String output) {
		Column column = new Column(this, name, input, output);
		columns.add(column);
		return column;
	}

	//
	// Data (state)
	//
	
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
		
		//
		// Update ranges of all tables
		//

		for(Table table : tables) {
			if(table.isPrimitive()) continue;
			table.addNewRange(); // Mark dirty as clean
			table.removeDelRange(); // Really remove old records
		}
		
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
	
	public Space(String name) {
		this.id = UUID.randomUUID();
		this.name = name;
		
		// Create primitive tables
		Table doubleType = createTable("Double");
		Table integerType = createTable("Integer");
		Table stringType = createTable("String");
	}

}
