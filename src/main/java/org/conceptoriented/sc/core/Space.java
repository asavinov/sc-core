package org.conceptoriented.sc.core;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.json.JSONObject;

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
	public void setName(String name) {
		this.name = name;
	}
	

	//
	// Environment
	//

	private ClassLoader classLoader; // Where Java code for UDF evaluators will be loaded from
	public ClassLoader getClassLoader() {
		return classLoader;
	}
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
	
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
	public Table getTableById(String id) {
        Optional<Table> ret = tables.stream().filter(x -> x.getId().toString().equals(id)).findAny();
        if(ret.isPresent()) {
        	return ret.get();
        }
        else {
    		return null;
        }
	}

	public Table createTable(String name) {
		Table table = new Table(this, name);
		tables.add(table);
		return table;
	}
	public Table createTableFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");

		// Check validity

		boolean isValid = true;
		if(name == null || name.isEmpty()) isValid = false;

		// Create

		if(isValid) {
			return this.createTable(name);
		}
		else {
			return null;
		}
	}
	public void updateTableFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");
		Table table = getTableById(id);

		// Update the properties

		table.setName(name);
	}
	public void deleteTable(String id) {
		Table table = getTableById(id);

		// Remove input columns
		List<Column> inColumns = columns.stream().filter(x -> x.getInput().equals(table)).collect(Collectors.<Column>toList());
		columns.removeAll(inColumns);
		
		// Remove output columns
		List<Column> outColumns = columns.stream().filter(x -> x.getOutput().equals(table)).collect(Collectors.<Column>toList());
		columns.removeAll(outColumns);
		
		// Remove table itself
		tables.remove(table);
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
        Optional<Column> ret = columns.stream().filter(x -> x.getInput().getName().equals(table) && x.getName().equals(column)).findAny();
        if(ret.isPresent()) {
        	return ret.get();
        }
        else {
    		return null;
        }
	}
	public Column getColumnById(String id) {
        Optional<Column> ret = columns.stream().filter(x -> x.getId().toString().equals(id)).findAny();
        if(ret.isPresent()) {
        	return ret.get();
        }
        else {
    		return null;
        }
	}

	public Column createColumn(String name, String input, String output) {
		Column column = new Column(this, name, input, output);
		columns.add(column);
		return column;
	}
	public Column createColumnFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");

		JSONObject input_table = obj.getJSONObject("input");
		String input_id = input_table.getString("id");
		Table input = this.getTableById(input_id);

		JSONObject output_table = obj.getJSONObject("output");
		String output_id = output_table.getString("id");
		Table output = this.getTableById(output_id);

		// Descriptor is either JSON object or JSON string with an object but we want to store a string
		String descr_string = null;
		Object jdescr = obj.get("descriptor");
		if(jdescr instanceof String) {
			descr_string = (String)jdescr;
		}
		else if(jdescr instanceof JSONObject) {
			descr_string = ((JSONObject) jdescr).toString();
		}

		//
		// Check validity
		//

		boolean isValid = true;
		if(name == null || name.isEmpty()) isValid = false;
		if(input == null) isValid = false;
		if(output == null) isValid = false;

		// Create

		if(isValid) {
			Column column = this.createColumn(name, input.getName(), output.getName());
			column.setDescriptor(descr_string);
			return column;
		}
		else {
			return null;
		}
	}
	public void updateColumnFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");
		Column column = getColumnById(id);

		JSONObject input_table = obj.getJSONObject("input");
		String input_id = input_table.getString("id");
		Table input = this.getTableById(input_id);

		JSONObject output_table = obj.getJSONObject("output");
		String output_id = output_table.getString("id");
		Table output = this.getTableById(output_id);

		// Descriptor is either JSON object or JSON string with an object but we want to store a string
		String descr_string = null;
		Object jdescr = obj.get("descriptor");
		if(jdescr instanceof String) {
			descr_string = (String)jdescr;
		}
		else if(jdescr instanceof JSONObject) {
			descr_string = ((JSONObject) jdescr).toString();
		}

		// Update the properties

		column.setName(name);
		column.setInput(input);
		column.setOutput(output);
		column.setDescriptor(descr_string);
	}
	public void deleteColumn(String id) {
		Column column = getColumnById(id);
		columns.remove(column);
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
			if(column.getEvaluator() == null) continue;
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
		
		// Create default class loader
		classLoader = ClassLoader.getSystemClassLoader();

		// Create primitive tables
		Table doubleType = createTable("Double");
		Table integerType = createTable("Integer");
		Table stringType = createTable("String");
	}

}
