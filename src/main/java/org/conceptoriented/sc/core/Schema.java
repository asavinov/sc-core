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

import org.apache.commons.lang3.StringUtils;
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
        Table ret = tables.stream().filter(x -> x.getName().equalsIgnoreCase(table)).findAny().orElse(null);
        return ret;
	}
	public Table getTableById(String id) {
        Table ret = tables.stream().filter(x -> x.getId().toString().equals(id)).findAny().orElse(null);
        return ret;
	}

	public Table createTable(String name) {
		Table table = new Table(this, name);
		tables.add(table);
		return table;
	}
	public Table createTableFromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		//
		// Validate properties
		//
		String id = obj.getString("id");

		String name = obj.getString("name");
		Table tab = getTable(name);
		if(tab != null) {
			throw new DcError(DcErrorCode.CREATE_ELEMENT, "Error creating table. ", "Name already exists. ");
		}
		if(!StringUtils.isAlphanumericSpace(name)) {
			throw new DcError(DcErrorCode.CREATE_ELEMENT, "Error creating table. ", "Name contains invalid characters. ");
		}

		long maxLength = obj.has("maxLength") && !obj.isNull("maxLength") ? obj.getLong("maxLength") : -1;

		//
		// Create
		//
		
		Table table = this.createTable(name);
		table.setMaxLength(maxLength);
		
		return table;
	}
	public void updateTableFromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		// Find table
		
		String id = obj.getString("id");
		Table table = getTableById(id);
		if(table == null) {
			throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating table. ", "Table not found. ");
		}
		
		//
		// Validate properties
		//
		if(obj.has("name")) {
			String name = obj.getString("name");
			Table tab = getTable(name);
			if(tab != null && tab != table) {
				throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating table. ", "Name already exists. ");
			}
			if(!StringUtils.isAlphanumericSpace(name)) {
				throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating table. ", "Name contains invalid characters. ");
			}
		}

		long maxLength = obj.has("maxLength") ? obj.getLong("maxLength") : 0;

		//
		// Update only properties which are present
		//

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
		List<Column> res = columns.stream().filter(x -> x.getInput().getName().equalsIgnoreCase(table)).collect(Collectors.<Column>toList());
		return res;
	}
	public Column getColumn(String table, String column) {
        Column ret = columns.stream().filter(x -> x.getInput().getName().equalsIgnoreCase(table) && x.getName().equalsIgnoreCase(column)).findAny().orElse(null);
        return ret;
	}
	public Column getColumnById(String id) {
        Column ret = columns.stream().filter(x -> x.getId().toString().equals(id)).findAny().orElse(null);
        return ret;
	}

	public Column createColumn(String name, String input, String output) {
		Column column = new Column(this, name, input, output);
		columns.add(column);
		return column;
	}
	public Column createColumnFromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");

		JSONObject input_table = obj.getJSONObject("input");
		String input_id = input_table.getString("id");
		Table input = this.getTableById(input_id);

		JSONObject output_table = obj.getJSONObject("output");
		String output_id = output_table.getString("id");
		Table output = this.getTableById(output_id);
		
		String name = obj.getString("name");
		Column col = this.getColumn(input.getName(), name);
		if(col != null) {
			throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name already exists. ");
		}
		if(!StringUtils.isAlphanumericSpace(name)) {
			throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name contains invalid characters. ");
		}

		String formula = (String)JSONObject.stringToValue(obj.has("formula") && !obj.isNull("formula") ? obj.getString("formula") : "");

		String accuformula = (String)JSONObject.stringToValue(obj.has("accuformula") && !obj.isNull("accuformula") ? obj.getString("accuformula") : "");
		String accutable = (String)JSONObject.stringToValue(obj.has("accutable") && !obj.isNull("accutable") ? obj.getString("accutable") : "");
		String accupath = (String)JSONObject.stringToValue(obj.has("accupath") && !obj.isNull("accupath") ? obj.getString("accupath") : "");

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

			column.setAccuformula(accuformula);
			column.setAccutable(accutable);
			column.setAccupath(accupath);

			column.setDescriptor(descr_string);

			return column;
		}
		else {
			return null;
		}
	}
	public void updateColumnFromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		Column column = getColumnById(id);

		JSONObject input_table = obj.getJSONObject("input");
		String input_id = input_table.getString("id");
		Table input = this.getTableById(input_id);

		JSONObject output_table = obj.getJSONObject("output");
		String output_id = output_table.getString("id");
		Table output = this.getTableById(output_id);

		if(obj.has("name")) {
			String name = obj.getString("name");
			Column col = this.getColumn(column.getInput().getName(), name);
			if(col != null && col != column) {
				throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name already exists. ");
			}
			if(!StringUtils.isAlphanumericSpace(name)) {
				throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name contains invalid characters. ");
			}
		}

		String formula = (String)JSONObject.stringToValue(obj.has("formula") && !obj.isNull("formula") ? obj.getString("formula") : "");
		
		String accuformula = (String)JSONObject.stringToValue(obj.has("accuformula") && !obj.isNull("accuformula") ? obj.getString("accuformula") : "");
		String accutable = (String)JSONObject.stringToValue(obj.has("accutable") && !obj.isNull("accutable") ? obj.getString("accutable") : "");
		String accupath = (String)JSONObject.stringToValue(obj.has("accupath") && !obj.isNull("accupath") ? obj.getString("accupath") : "");

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
		// Update only the properties which have been provided
		//

		if(obj.has("input")) column.setInput(input);
		if(obj.has("output")) column.setOutput(output);
		if(obj.has("name")) column.setName(obj.getString("name"));

		if(obj.has("formula")) column.setFormula(formula);

		if(obj.has("accuformula")) column.setAccuformula(accuformula);
		if(obj.has("accutable")) column.setAccutable(accutable);
		if(obj.has("accupath")) column.setAccupath(accupath);

		if(obj.has("descriptor")) column.setDescriptor(descr_string);
	}
	public void deleteColumn(String id) {
		Column column = getColumnById(id);
		columns.remove(column);
	}

	//
	// Dependencies
	//
	
	/**
	 * For each column, we store all other columns it directly depends on, that is, columns that it directly uses in its formula
	 */
	protected Map<Column,List<Column>> dependencies = new HashMap<Column,List<Column>>();
	public List<Column> getDependency(Column column) {
		return dependencies.get(column);
	}
	public void setDependency(Column column, List<Column> deps) {
		dependencies.put(column, deps);
	}

	// Return all columns with no definition which therefore are supposed to be always clean and do not need evaluation
	protected List<Column> getStartingColumns() {
		List<Column> res = columns.stream().filter(x -> x.getFormula() == null && x.getDescriptor() == null).collect(Collectors.<Column>toList());
		return res;
	}

	// Get all column which have all their dependencies covered by the specified columns in the parameter list.
	// We get all columns that directly depend on the specified columns.
	// If the parameter includes all already evaluated columns then the output has columns that are ready to be evaluated on the next step.
	// Essentially, we follow the dependency graph by generating next layer of columns in the output based on the previous layer of columns in the parameter.
	protected List<Column> getNextColumns(List<Column> previousColumns) {
		List<Column> res = new ArrayList<Column>();
		
		for(Map.Entry<Column, List<Column>> entry : dependencies.entrySet()) {
			Column col = entry.getKey();
			List<Column> deps = entry.getValue();
			if(deps == null) continue; // Non-evaluatable (no formula or error)
			if(previousColumns.contains(col)) continue; // Skip already evaluated columns

			// Find at least one column with errors which are not suitable for propagation
			Column errCol = deps.stream().filter(x -> x.getStatus() != null && x.getStatus().code != DcErrorCode.NONE).findAny().orElse(null);
			if(errCol != null) continue;
			
			if(previousColumns.containsAll(deps)) { // All deps have to be evaluated (non-dirty)
				res.add(col); 
			}
		}
		
		return res;
	}


	/**
	 * Parse and bind all column formulas in the schema. The result is stored in the state property of each column.
	 */
	public void translate() {
		
		// Translate all columns individually by filling in their own dependencies and their own status
		// Propagate translation status through the graph
		
		// Result deps are up-to-date
		// Translation own status is up-to-date
		// Translation dependency status is up-to-date
		
		// How does it influence data status
		

		
		
		
		// Complete translation means: parse all individual column formulas and bind all individual column formulas
		// Dependencies are reset, that is, empty and we start from having no dependencies
		// Results: 
		// - dependencies are generated (even if some formulas have errors), 
		// - all column statuses are set (with errors if any or success or anything that can be shown as status)
		// - 
		// What if a column has not been changed? 
		// What happens with data status? For example, does it mean that all column data are marked dirty (even if the column has not changed)?

		// One option is to translate only columns marked as 'formula changed' or 'name changed' (name can influence other columns). 
		// In other words, in addition to data status (new, deleted, clean) we need a kind of schema/formula status which is important for translation phase.
		// Note that changing formula makes automatically data dirty. However, it also makes this column formula dirty. What is the difference?
		
		// Track status of: column status, e.g., color and message, column evaluate status (dirty, errors), table status (new rows)
		// Update (dirty) status: column name changed, column formula changed (expr, fact, path), rows added (data status)
		// What we want to display? Status for each column: ...
		
		// Display translate status with explicit action, that is, 
		// we need to update Translate status of all columns (and other elements) after any change automatically.
		// Changes: name and property changes, formula and properties changes 
		// Formula/translation Status: no-formula (not evaluable, no deps), valid/invalid own formula + valid/invalid dependant formulas -> readiness to evaluate
		// After every column edit, we update - deps, own formulas, deps erro propagation - and store the new statuses so that they can be requested
		// Alternatively, we can simply store all changes, while the complete status is computed by means of explicit call of Translate function.
		
		// What about dirty status of data? 
		// If it is separate, then it is clean after (successfull) evaluation of this column
		// If no formula, then no status (non-evaluable, e.g., always green or gray). Determined by property like HasFormula() 
		// If formula is changed then it and all its followers are marked dirty. It is done directly in the data.
		// If rows are added then all column of this table are marked dirty (which can be determined by dirty status of table/column data)
		
		
	}

	/**
	 * Evaluate all columns of the schema. The result is stored in the state property of each column.
	 * Only new (dirty) rows will be evaluated and made clean (non-dirty). 
	 */
	public void evaluate() {

		List<Column> readyColumns = getStartingColumns(); // Start from non-evaluatable columns (no definition)
		List<Column> nextColumns = getNextColumns(readyColumns); // First iteration
		while(nextColumns.size() > 0) {
			// Evaluate all columns that can be evaluated
			for(Column col : nextColumns) {
				if(col.getStatus() == null || col.getStatus().code == DcErrorCode.NONE) {
					col.evaluate();
					readyColumns.add(col);
				}
			}
			// Next iteration
			nextColumns = getNextColumns(readyColumns);
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
	public static Schema fromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");

		String name = obj.getString("name");
		if(!StringUtils.isAlphanumericSpace(name)) {
			throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating schema. ", "Name contains invalid characters. ");
		}

		//
		// Create
		//
		
		Schema schema = new Schema(name);
		return schema;
	}
	public void updateFromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		//
		// Extract parameters and check validity
		//
		
		String id = obj.getString("id");

		if(obj.has("name")) {
			if(!StringUtils.isAlphanumericSpace(name)) {
				throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name contains invalid characters. ");
			}
		}

		//
		// Update the properties
		//

		if(obj.has("name")) this.setName(obj.getString("name"));
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
