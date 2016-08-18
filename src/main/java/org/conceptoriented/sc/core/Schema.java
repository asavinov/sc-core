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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.json.JSONObject;

/**
 * Stream schema stores the complete data state and is able to consistently update it. 
 */
public class Schema {
	
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
        if(ret.isPresent()) return ret.get();
        else return null;
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

		long maxLength = obj.has("maxLength") && !obj.isNull("maxLength") ? obj.getLong("maxLength") : -1;

		// Check validity

		boolean isValid = true;
		if(name == null || name.isEmpty()) isValid = false;
		
		if(!isValid) return null;

		// Create
		
		Table table = this.createTable(name);
		table.setMaxLength(maxLength);
		
		return table;
	}
	public void updateTableFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Find table
		
		String id = obj.getString("id");
		Table table = getTableById(id);
		if(table == null) return;
		
		// Update only properties which are present

		if(obj.has("name")) table.setName(obj.getString("name"));
		if(obj.has("maxLength")) table.setMaxLength(obj.getLong("maxLength"));
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
        if(ret.isPresent()) return ret.get();
        else return null;
	}
	public Column getColumnById(String id) {
        Optional<Column> ret = columns.stream().filter(x -> x.getId().toString().equals(id)).findAny();
        if(ret.isPresent()) return ret.get();
        else return null;
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
		
		String formula = (String)JSONObject.stringToValue(obj.has("formula") && !obj.isNull("formula") ? obj.getString("formula") : "");

		// Descriptor is either JSON object or JSON string with an object but we want to store a string
		String descr_string = "";
		if(obj.has("descriptor") && !obj.isNull("descriptor")) {
			Object jdescr = obj.get("descriptor");
			if(jdescr instanceof String) {
				descr_string = (String)jdescr;
			}
			else if(jdescr instanceof JSONObject) {
				descr_string = ((JSONObject) jdescr).toString();
			}
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
			column.setFormula(formula);
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

		String formula = (String)JSONObject.stringToValue(obj.has("formula") && !obj.isNull("formula") ? obj.getString("formula") : "");
		
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
		column.setFormula(formula);
		column.setDescriptor(descr_string);
	}
	public void deleteColumn(String id) {
		Column column = getColumnById(id);
		columns.remove(column);
	}

	//
	// Data (state)
	//
	
	protected Map<Column,List<Column>> dependencies = new HashMap<Column,List<Column>>();
	public List<Column> getDependency(Column column) {
		return dependencies.get(column);
	}
	public void setDependency(Column column, List<Column> deps) {
		dependencies.put(column, deps);
	}

	protected List<Column> getPassiveColumns() {
		// Return all columns without definition (which need not be evaluated and always non-dirty)
		List<Column> res = columns.stream().filter(x -> x.getFormula() == null && x.getDescriptor() == null).collect(Collectors.<Column>toList());
		return res;
	}

	// The parameter is a list of already evaluated (non-dirty) columns
	// Output is a list of columns which are ready to be evaluated because all their dependencies are evaluated
	protected List<Column> getCanEvaluate(List<Column> evaluated) {
		List<Column> res = new ArrayList<Column>();
		
		for(Map.Entry<Column, List<Column>> entry : dependencies.entrySet()) {
			Column col = entry.getKey();
			List<Column> deps = entry.getValue();
			if(evaluated.contains(col)) continue; // Skip already evaluated columns
			if(evaluated.containsAll(deps)) { // All deps have to be evaluated (non-dirty)
				res.add(col); 
			}
		}
		
		return res;
	}

	public void evaluate() {
		
		//
		// Evaluate the schema. Make again consistent (non-dirty).
		// Bring the state back to consistent state by re-computing the values which are known to be dirty.
		//

		// Build a list/graph of columns to be evaluated. Either in the sequence of creation, or using dependencies.
		// Evaluate each column individually from this structure. 
		// Any column has to provide an evaluation function which knows how to compute the output value.

		// A column graph includes all columns that can be (and hence have to be) evaluated. Non-evaluabable columns are not included.
		// A graph has a list of origin columns which can be evaluated independently. 

		// We need a function which takes two inputs: a graph of dependencies, and a list of already evaluated columns.
		// It returns a column if all its direct dependencies are evaluated (which are supposed to be evaluated only if their direct deps are evaluated)
		// If no evaluated columns are provided, then only columns with empty deps are returned. 
		// Input evaluated columns are not returned. 
		
 		// Start from input 0 (no evaluated columns)
		// After some columns has been evaluated, it is added to the list of evaluated columns and can be again used for input
		// So after each addition of evaluated columns the return columns will decrease down to 0

		List<Column> evaluated = getPassiveColumns(); // Already evaluated. Initially non-dirty columns without definition
		List<Column> toBeEvaluated = getCanEvaluate(evaluated);
		while(toBeEvaluated.size() > 0) {
			// Evaluate all columns that can be evaluated
			for(Column col : toBeEvaluated) {
				col.evaluate();
				evaluated.add(col);
			}
			// Next iteration
			toBeEvaluated = getCanEvaluate(evaluated);
		}

		//
		// Update ranges of all tables
		//

		for(Table table : tables) {
			if(table.isPrimitive()) continue;
			table.markNewAsClean(); // Mark dirty as clean
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
	public static Schema fromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");

		// Check validity

		boolean isValid = true;
		if(name == null || name.isEmpty()) isValid = false;

		// Create
		
		if(isValid) return new Schema(name);
		else return null;
	}
	public void updateFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");

		// Check validity

		boolean isValid = true;
		if(name == null || name.isEmpty()) isValid = false;

		// Update the properties

		this.setName(name);
	}
	
	@Override
	public String toString() {
		return "[" + name + "]";
	}
	
	public Schema(String name) {
		this.id = UUID.randomUUID();
		this.name = name;
		
		// Create default class loader
		classLoader = ClassLoader.getSystemClassLoader();

		// Create primitive tables
		Table doubleType = createTable("Double");
		Table stringType = createTable("String");
	}

}
