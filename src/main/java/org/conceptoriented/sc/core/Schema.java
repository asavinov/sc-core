package org.conceptoriented.sc.core;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.google.common.io.Files;

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
		return this.tables;
	}
	public Table getTable(String table) {
        Table ret = this.tables.stream().filter(x -> x.getName().equalsIgnoreCase(table)).findAny().orElse(null);
        return ret;
	}
	public Table getTableById(String id) {
        Table ret = this.tables.stream().filter(x -> x.getId().toString().equals(id)).findAny().orElse(null);
        return ret;
	}

	public Table createTable(String name) {
		Table tab = this.getTable(name);
		if(tab != null) return tab; // Already exists

		tab = new Table(this, name);
		this.tables.add(tab);
		return tab;
	}
	public Table createTableFromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		//
		// Validate properties
		//
		String id = obj.getString("id");
		String name = obj.getString("name");

		Table tab = this.getTable(name);
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
		
		tab = this.createTable(name);
		tab.setMaxLength(maxLength);
		
		return tab;
	}
    public Table createFromCsvFile(String fileName, boolean hasHeaderRecord) {
        String tableName = null;
        File file = new File(fileName);
        tableName = file.getName();
        tableName = Files.getNameWithoutExtension(tableName);

        // Read column names from CSV
        List<String> colNames = Schema.readColumnNamesFromCsvFile(fileName);
        
        // Read Records from CSV
        List<Record> records = Record.fromCsvFile(fileName, colNames, true);
        
        // Get column types
        List<String> colTypes = Utils.recommendTypes(colNames, records);

        // Create/append table with file name
		Table tab = this.createTable(tableName);
        
        // Append columns
		this.createColumns(tab.getName(), colNames, colTypes);
        
        // Append records to this table
        tab.append(records, null);
        
        return tab;
    }
    public Table createFromCsvLines(String tableName, String csvLines, String params) {
		if(params == null || params.isEmpty()) params = "{}";
		
		JSONObject paramsObj = new JSONObject(params);
		List<String> lines = new ArrayList<String>(Arrays.asList(csvLines.split("\\r?\\n")));

        // Read column names from CSV
		String headerLine = lines.get(0);
		List<String> colNames = Utils.csvLineToList(headerLine, paramsObj);
        
        // Read Records from CSV
		List<Record> records = Record.fromCsvList(csvLines, params);
        
        // Create/append table with file name
		Table tab = this.createTable(tableName);

		// Append columns if necessary
		if(paramsObj.optBoolean("createColumns")) {
	        List<String> colTypes = Utils.recommendTypes(colNames, records);
			this.createColumns(tab.getName(), colNames, colTypes);
		}
        
        // Append records to this table
        tab.append(records, null);
        
        return tab;
    }


    public void updateTableFromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		// Find table
		
		String id = obj.getString("id");
		Table tab = getTableById(id);
		if(tab == null) {
			throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating table. ", "Table not found. ");
		}
		
		//
		// Validate properties
		//
		if(obj.has("name")) {
			String name = obj.getString("name");
			Table t = getTable(name);
			if(t != null && t != tab) {
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

		if(obj.has("name")) tab.setName(obj.getString("name"));
		if(obj.has("maxLength")) tab.setMaxLength(obj.getLong("maxLength"));
	}

    public void deleteTable(String id) {
		Table tab = getTableById(id);

		// Remove input columns
		List<Column> inColumns = this.columns.stream().filter(x -> x.getInput().equals(tab)).collect(Collectors.<Column>toList());
		this.columns.removeAll(inColumns);
		
		// Remove output columns
		List<Column> outColumns = this.columns.stream().filter(x -> x.getOutput().equals(tab)).collect(Collectors.<Column>toList());
		this.columns.removeAll(outColumns);
		
		// Remove table itself
		this.tables.remove(tab);
	}

	//
	// Columns
	//

	private List<Column> columns = new ArrayList<Column>();
	public List<Column> getColumns() {
		return this.columns;
	}
	public List<Column> getColumns(String table) {
		List<Column> res = this.columns.stream().filter(x -> x.getInput().getName().equalsIgnoreCase(table)).collect(Collectors.<Column>toList());
		return res;
	}
	public Column getColumn(String table, String column) {
        Column ret = this.columns.stream().filter(x -> x.getInput().getName().equalsIgnoreCase(table) && x.getName().equalsIgnoreCase(column)).findAny().orElse(null);
        return ret;
	}
	public Column getColumnById(String id) {
        Column ret = this.columns.stream().filter(x -> x.getId().toString().equals(id)).findAny().orElse(null);
        return ret;
	}

	public Column createColumn(String input, String name, String output) {
		Column col = new Column(this, name, input, output);
		this.columns.add(col);
		return col;
	}
	public List<Column> createColumns(String input, List<String> names, List<String> outputs) {
		List<Column> cols = new ArrayList<Column>();
		
		for(int i=0; i<names.size(); i++) {
			Column col = this.getColumn(input, names.get(i));
			if(col != null) continue; // Already exists

			col = this.createColumn(input, names.get(i), outputs.get(i));
			cols.add(col);
		}

		return cols;
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
			throw new DcError(DcErrorCode.CREATE_ELEMENT, "Error creating column. ", "Name already exists. ");
		}
		if(!StringUtils.isAlphanumericSpace(name)) {
			throw new DcError(DcErrorCode.CREATE_ELEMENT, "Error creating column. ", "Name contains invalid characters. ");
		}

		// We do not process status (it is always result of the backend)
		// We do not process dirty (it is always result of the backend)
		
		DcColumnKind kind = obj.has("kind") ? DcColumnKind.fromInt(obj.getInt("kind")) : DcColumnKind.AUTO;
		
		String formula = obj.has("formula") && !obj.isNull("formula") ? obj.getString("formula") : "";

		String accuformula = obj.has("accuformula") && !obj.isNull("accuformula") ? obj.getString("accuformula") : "";
		String accutable = obj.has("accutable") && !obj.isNull("accutable") ? obj.getString("accutable") : "";
		String accupath = obj.has("accupath") && !obj.isNull("accupath") ? obj.getString("accupath") : "";

		// Descriptor is either JSON object or JSON string with an object but we want to store a string
		String descr_string = "";
		if(obj.has("descriptor")) {
			Object jdescr = !obj.isNull("descriptor") ? obj.get("descriptor") : "";
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
			col = this.createColumn(input.getName(), name, output.getName());

			col.setKind(kind);

			col.setFormula(formula);

			col.setAccuformula(accuformula);
			col.setAccutable(accutable);
			col.setAccupath(accupath);

			col.setDescriptor(descr_string);
			
			if(!col.isDerived()) { // Columns without formula (non-evalatable) are clean
				col.setDirty(false);
			}

			return col;
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

		// We do not process status (it is always result of the backend)
		// We do not process dirty (it is always result of the backend)
		
		DcColumnKind kind = obj.has("kind") ? DcColumnKind.fromInt(obj.getInt("kind")) : DcColumnKind.AUTO;
		
		String formula = obj.has("formula") && !obj.isNull("formula") ? obj.getString("formula") : "";
		
		String accuformula = obj.has("accuformula") && !obj.isNull("accuformula") ? obj.getString("accuformula") : "";
		String accutable = obj.has("accutable") && !obj.isNull("accutable") ? obj.getString("accutable") : "";
		String accupath = obj.has("accupath") && !obj.isNull("accupath") ? obj.getString("accupath") : "";

		// Descriptor is either JSON object or JSON string with an object but we want to store a string
		String descr_string = null;
		if(obj.has("descriptor")) {
			Object jdescr = !obj.isNull("descriptor") ? obj.get("descriptor") : "";
			if(jdescr instanceof String) {
				descr_string = (String)jdescr;
			}
			else if(jdescr instanceof JSONObject) {
				descr_string = ((JSONObject) jdescr).toString();
			}
		}

		//
		// Update only the properties which have been provided
		//

		if(obj.has("input")) column.setInput(input);
		if(obj.has("output")) column.setOutput(output);
		if(obj.has("name")) column.setName(obj.getString("name"));

		if(obj.has("kind")) column.setKind(kind);

		if(obj.has("formula")) column.setFormula(formula);

		if(obj.has("accuformula")) column.setAccuformula(accuformula);
		if(obj.has("accutable")) column.setAccutable(accutable);
		if(obj.has("accupath")) column.setAccupath(accupath);

		if(obj.has("descriptor")) column.setDescriptor(descr_string);
	}

	public void deleteColumn(String id) {
		Column col = this.getColumnById(id);
		this.columns.remove(col);
	}

	public static List<String> readColumnNamesFromCsvFile(String fileName) {
		List<String> columnNames = new ArrayList<String>();

		try {
            File file = new File(fileName);

            Reader in = new FileReader(file);
            Iterable<CSVRecord> csvRecs = CSVFormat.EXCEL.parse(in);

            for (CSVRecord csvRec : csvRecs) {
            	for(int i=0; i<csvRec.size(); i++) {
            		columnNames.add(csvRec.get(i));
                }
            	break; // Only one record is needed
            }

            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

		return columnNames;
	}

	//
	// Dependencies
	//
	
	/**
	 * For each column, we store all other columns it directly depends on, that is, columns that it directly uses in its formula
	 */
	protected Map<Column,List<Column>> dependencies = new HashMap<Column,List<Column>>();
	public List<Column> getParentDependencies(Column col) {
		return this.dependencies.get(col);
	}
	public void setParentDependencies(Column col, List<Column> deps) {
		this.dependencies.put(col, deps);
	}
	public List<Column> getChildDependencies(Column col) {
		// Return all columns which point to the specified column as a dependency, that is, which have this column in its deps
		List<Column> res = this.columns.stream().filter(x -> this.getParentDependencies(x) != null && this.getParentDependencies(x).contains(col)).collect(Collectors.<Column>toList());
		return res;
	}
	public void emptyDependencies() { // Reset. Normally before finding them.
		this.dependencies = new HashMap<Column,List<Column>>();
	}

	// Return all columns with no definition which therefore are supposed to be always clean and do not need evaluation
	protected List<Column> getStartingColumns() {
		List<Column> res = this.columns.stream().filter(x -> !x.isDerived()).collect(Collectors.<Column>toList());
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

			// If it has errors then exclude it (cannot be evaluated)
			if(col.getStatus() != null && col.getStatus().code != DcErrorCode.NONE) {
				continue;
			}

			// If one of its dependencies has errors then exclude it (cannot be evaluated)
			Column errCol = deps.stream().filter(x -> x.getStatus() != null && x.getStatus().code != DcErrorCode.NONE).findAny().orElse(null);
			if(errCol != null) continue;
			
			if(previousColumns.containsAll(deps)) { // All deps have to be evaluated (non-dirty)
				res.add(col); 
			}
		}
		
		return res;
	}

	protected List<Column> getAllNextColumns(List<Column> previousColumns) {
		List<Column> res = new ArrayList<Column>();
		
		for(Map.Entry<Column, List<Column>> entry : dependencies.entrySet()) {
			Column col = entry.getKey();
			List<Column> deps = entry.getValue();
			if(deps == null) continue; // Non-evaluatable (no formula or error)
			if(previousColumns.contains(col)) continue; // Skip already evaluated columns

			if(previousColumns.containsAll(deps)) { // All deps have to be evaluated (non-dirty)
				res.add(col); 
			}
		}
		
		return res;
	}

	/**
	 * Parse and bind all column formulas in the schema. 
	 * All dependencies are computed and made up-to-date.
	 * The updated result of translation is stored in individual columns.
	 */
	public void translate() {
		
		//
		// Translate individual columns and build dependency graph
		//

		this.emptyDependencies(); // Reset
		for(Column col : this.columns) {
			col.translate();
		}
		
		//
		// Propagate translation status (errors) through dependency graph
		//
		
		List<Column> readyColumns = new ArrayList<Column>(); // Already evaluated
		List<Column> nextColumns = this.getStartingColumns(); // Initialize. First iteration with column with no dependency formulas. 
		while(nextColumns.size() > 0) {
			for(Column col : nextColumns) {
				if(col.getStatus() == null || col.getStatus().code == DcErrorCode.NONE) {
					// If there is at least one error in dependencies then mark this as propagated error
					List<Column> deps = this.getParentDependencies(col);
					if(deps == null) { 
						readyColumns.add(col);
						continue;
					}
					// If at least one dependency has errors then this column is not suitable for propagation
					Column errCol = deps.stream().filter(x -> x.getStatus() != null && x.getStatus().code != DcErrorCode.NONE).findAny().orElse(null);
					if(errCol != null) { // Mark this column as having propagated error
						if(errCol.getStatus().code == DcErrorCode.PARSE_ERROR || errCol.getStatus().code == DcErrorCode.PARSE_PROPAGATION_ERROR)
							col.mainExpr.status = new DcError(DcErrorCode.PARSE_PROPAGATION_ERROR, "Propagated parse error.", "Error in the column: '" + errCol.getName() + "'");
						else if(errCol.getStatus().code == DcErrorCode.BIND_ERROR || errCol.getStatus().code == DcErrorCode.BIND_PROPAGATION_ERROR)
							col.mainExpr.status = new DcError(DcErrorCode.BIND_PROPAGATION_ERROR, "Propagated bind error.", "Error in the column: '" + errCol.getName() + "'");
						else if(errCol.getStatus().code == DcErrorCode.EVALUATE_ERROR || errCol.getStatus().code == DcErrorCode.EVALUATE_PROPAGATION_ERROR)
							col.mainExpr.status = new DcError(DcErrorCode.EVALUATE_PROPAGATION_ERROR, "Propagated evaluation error.", "Error in the column: '" + errCol.getName() + "'");
						else
							col.mainExpr.status = new DcError(DcErrorCode.GENERAL, "Propagated error.", "Error in the column: '" + errCol.getName() + "'");
					}
				}
				readyColumns.add(col);
			}
			nextColumns = this.getAllNextColumns(readyColumns); // Next iteration
		}
		
		//
		// Find columns with cyclic dependencies
		//
		for(Column col : this.getColumns()) {
			if(readyColumns.contains(col)) continue;
			
			// If a column has not been covered during propagation then it belongs to a cycle. The cycle itself is not found by this procedure. 

			if(col.getStatus() == null || col.getStatus().code == DcErrorCode.NONE) {
				if(col.mainExpr == null) col.mainExpr = new ExprNode(); // Wrong use. Should not happen.
				col.mainExpr.status = new DcError(DcErrorCode.DEPENDENCY_CYCLE_ERROR, "Cyclic dependency.", "This column formula depends on itself by using other columns which depend on it.");
			}
		}

	}

	/**
	 * Evaluate all columns of the schema. The result is stored in the state property of each column.
	 * Only new (dirty) rows will be evaluated and made clean (non-dirty). 
	 */
	public void evaluate() {

		List<Column> readyColumns = new ArrayList<Column>(); // Already evaluated
		List<Column> nextColumns = this.getStartingColumns(); // Initialize. First iteration with column with no dependency formulas. 
		while(nextColumns.size() > 0) {
			for(Column col : nextColumns) {
				if(col.getStatus() == null || col.getStatus().code == DcErrorCode.NONE) {
					col.evaluate();
					readyColumns.add(col);
				}
			}
			nextColumns = this.getNextColumns(readyColumns); // Next iteration
		}

		//
		// Update ranges of all tables
		//

		for(Table tab : tables) {
			if(tab.isPrimitive()) continue;
			tab.markNewAsClean(); // Mark dirty as clean
			tab.removeDelRange(); // Really remove old records
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
			String name = obj.getString("name");
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
