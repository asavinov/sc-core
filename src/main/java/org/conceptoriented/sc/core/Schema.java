package org.conceptoriented.sc.core;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
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
	// Rules of evaluation and population for the whole schema
	//
	
	// Evaluate automatically after this time elapsed after the last append.
	// It is time duration during which the schema is allowed to be dirty and after this period the system will try to make it up-to-date by performing evaluation.
	// The system tries to keep dirty status only during some limited time, and clean it after the specified time.
	public long afterAppend = -1;

	public boolean autoevaluationNeeded() {
		if(this.afterAppend < 0) return false; // Auto-evaluation is turned off (evaluate only manually). Also null or MAX could be used for other units.
		
		if(this.afterAppend == 0) return true; // Immediate evaluation after every append (no dirty state)

		if(this.durationSinceLastEvaluate().toNanos() < Duration.ofMillis(afterAppend).toNanos()) {
			return false;
		}
		return true;
	}
	
	public boolean autoEvaluate() {

		if(!this.autoevaluationNeeded()) {
			return false; // No auto-evaluation needed
		}
		
		//
		// Evaluate all
		//

		// Mark all columns dirty (non-evaluated) - otherwise evaluation will not do anything because it thinks that the functions are up-ot-date
		this.getColumns().forEach(x -> x.getData().isChanged = true);

		this.translate();
		this.evaluate(); // Evaluate
		
		return true;
	}

	//
	// Every regular interval aligned with calendar units like every 1 minute or every 1 second. 
	// The system will try to do evaluation exactly after each calendar time unit. 
	// After each time tick, the system will trigger evaluation (if not already scheduled or running).
	
	//
	// After the specified arbitrary time after the last evaluation
	// The system tries to do evaluations in the specified time after the last evaluation.
	// It may happen that no evaluation is required because the status is clean
	protected long minEvaluationFrequency = -1;  

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
		if(!Utils.validElementName(name)) {
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
        
        // Auto-evaluation if needed
        this.autoEvaluate();

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
        
        // Auto-evaluation if needed
        this.autoEvaluate();
        
        return tab;
    }


    public void updateTableFromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		// Find table
		
		String id = obj.getString("id");
		Table tab = this.getTableById(id);
		if(tab == null) {
			throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating table. ", "Table not found. ");
		}
		
		//
		// Validate properties
		//
		if(obj.has("name")) {
			String name = obj.getString("name");
			Table t = this.getTable(name);
			if(t != null && t != tab) {
				throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating table. ", "Name already exists. ");
			}
			if(!Utils.validElementName(name)) {
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
		if(!Utils.validElementName(name)) {
			throw new DcError(DcErrorCode.CREATE_ELEMENT, "Error creating column. ", "Name contains invalid characters. ");
		}

		// We do not process status (it is always result of the backend)
		// We do not process dirty (it is always result of the backend)
		
		DcColumnKind kind = obj.has("kind") ? DcColumnKind.fromInt(obj.getInt("kind")) : DcColumnKind.AUTO;
		
		String calcFormula = obj.has("calcFormula") && !obj.isNull("calcFormula") ? obj.getString("calcFormula") : "";

		String linkFormula = obj.has("linkFormula") && !obj.isNull("linkFormula") ? obj.getString("linkFormula") : "";

		String initFormula = obj.has("initFormula") && !obj.isNull("initFormula") ? obj.getString("initFormula") : "";
		String accuFormula = obj.has("accuFormula") && !obj.isNull("accuFormula") ? obj.getString("accuFormula") : "";
		String accuTable = obj.has("accuTable") && !obj.isNull("accuTable") ? obj.getString("accuTable") : "";
		String accuPath = obj.has("accuPath") && !obj.isNull("accuPath") ? obj.getString("accuPath") : "";

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

			// Always create a new definition object
			col.setDefinitionCalc(new ColumnDefinitionCalc(calcFormula, col.expressionKind));
			col.setDefinitionLink(new ColumnDefinitionLink(linkFormula, col.expressionKind));
			col.setDefinitionAccu(new ColumnDefinitionAccu(initFormula, accuFormula, null, accuTable, accuPath, col.expressionKind));

			if(!col.isDerived()) { // Columns without formula (non-evalatable) are clean
				col.setFormulaChange(false);
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
		Column column = this.getColumnById(id);

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
			if(!Utils.validElementName(name)) {
				throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name contains invalid characters. ");
			}
		}

		// We do not process status (it is always result of the backend)
		// We do not process dirty (it is always result of the backend)
		
		DcColumnKind kind = obj.has("kind") ? DcColumnKind.fromInt(obj.getInt("kind")) : DcColumnKind.AUTO;
		
		String calcFormula = obj.has("calcFormula") && !obj.isNull("calcFormula") ? obj.getString("calcFormula") : "";
		
		String linkFormula = obj.has("linkFormula") && !obj.isNull("linkFormula") ? obj.getString("linkFormula") : "";
		
		String initFormula = obj.has("initFormula") && !obj.isNull("initFormula") ? obj.getString("initFormula") : "";
		String accuFormula = obj.has("accuFormula") && !obj.isNull("accuFormula") ? obj.getString("accuFormula") : "";
		String accuTable = obj.has("accuTable") && !obj.isNull("accuTable") ? obj.getString("accuTable") : "";
		String accuPath = obj.has("accuPath") && !obj.isNull("accuPath") ? obj.getString("accuPath") : "";

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

		// Always create a new definition object
		if(obj.has("calcFormula")) 
			column.setDefinitionCalc(new ColumnDefinitionCalc(calcFormula, column.expressionKind));
		if(obj.has("linkFormula")) 
			column.setDefinitionLink(new ColumnDefinitionLink(linkFormula, column.expressionKind));
		if(obj.has("initFormula") || obj.has("accuFormula") || obj.has("initTable") || obj.has("initPath")) 
			column.setDefinitionAccu(new ColumnDefinitionAccu(initFormula, accuFormula, null, accuTable, accuPath, column.expressionKind));
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
	// Translation (parse and bind formulas, prepare for evaluation)
	//
	
	/**
	 * Parse, bind and build all column formulas in the schema. 
	 * Generate dependencies.
	 */
	public void translate() {
		// Translate individual columns
		for(Column col : this.columns) {
			if(!col.isDerived()) continue;
			col.translate();
		}
	}

	//
	// Evaluate (re-compute dirty, selected or all function outputs)
	//
	
	Instant evaluateTime = Instant.now(); // Last time the evaluation has been performed (successfully finished)
	public Instant getEvaluateTime() {
		return this.evaluateTime;
	}
	public void setEvaluateTime() {
		this.evaluateTime = Instant.now();
	}
	public Duration durationSinceLastEvaluate() {
		return Duration.between(this.evaluateTime, Instant.now());
	}
	
	/**
	 * Evaluate all columns of the schema which can be evaluated and need evaluation (dirty output).
	 * 
	 * The order of column evaluation is determined by the dependency graph.
	 * Can evaluate depends on the error status: translate errors, evaluate errors, self-dependence errors, and these errors in dependencies.
	 * Need evaluate depends on formula changes, data output changes, set changes, and these changes in dependencies.
	 * 
	 * Finally, the status of each evaluated column is cleaned (made up-to-date). 
	 */
	public void evaluate() {
		
		List<Column> done = new ArrayList<Column>();
		for(List<Column> cols = this.getStartingColumns(); cols.size() > 0; cols = this.getNextColumnsEvaluatable(done)) { // Loop on expansion layers of dependencies forward
			for(Column col : cols) {
				if(!col.isDerived()) continue;
				// TODO: Detect also evaluate errors that could have happened before in this same evaluate loop and prevent this column from evaluation
				// Evaluate errors have to be also taken into account when generating next layer of columns
				DcError de = col.getTranslateError();
				if(de == null || de.code == DcErrorCode.NONE) {
					col.evaluate();
				}
			}
			done.addAll(cols);
		}

		this.setEvaluateTime(); // Store the time of evaluation
	}
	
	//
	// Dependency graph (needed to determine the order of column evaluations, generated by translation)
	//
	
	protected List<Column> getStartingColumns() { // Return all columns which do not depend on other columns (starting nodes in the dependency graph)
		List<Column> res = this.columns.stream().filter(x -> x.isStartingColumn()).collect(Collectors.<Column>toList());
		return res;
	}
	protected List<Column> getNextDependencies(Column col) { // Return all columns have the specified column in their dependencies (but can depend also on other columns)
		List<Column> res = this.columns.stream().filter(x -> x.getDependencies() != null && x.getDependencies().contains(col)).collect(Collectors.<Column>toList());
		return res;
	}
	protected List<Column> getNextColumns(List<Column> previousColumns) { // Get columns with all their dependencies in the specified list
		List<Column> ret = new ArrayList<Column>();
		
		for(Column col : this.columns) {

			if(previousColumns.contains(col)) continue; // Already in the list. Ccan it really happen without cycles?
			List<Column> deps = col.getDependencies();
			if(deps == null) continue; // Something wrong

			if(previousColumns.containsAll(deps)) { // All column dependencies are in the list
				ret.add(col); 
			}
		}
		
		return ret;
	}
	protected List<Column> getNextColumnsEvaluatable(List<Column> previousColumns) { // Get columns with all their dependencies in the specified list and having no translation errors (own or inherited)
		List<Column> ret = new ArrayList<Column>();
		
		for(Column col : this.columns) {
			if(previousColumns.contains(col)) continue;  // Already in the list. Ccan it really happen without cycles?
			List<Column> deps = col.getDependencies();
			if(deps == null) continue; // Something wrong

			// If it has errors then exclude it (cannot be evaluated)
			if(col.getTranslateError() != null && col.getTranslateError().code != DcErrorCode.NONE) {
				continue;
			}

			// If one of its dependencies has errors then exclude it (cannot be evaluated)
			Column errCol = deps.stream().filter(x -> x.getTranslateError() != null && x.getTranslateError().code != DcErrorCode.NONE).findAny().orElse(null);
			if(errCol != null) continue;
			
			if(previousColumns.containsAll(deps)) { // All deps have to be evaluated (non-dirty)
				ret.add(col); 
			}
		}
		
		return ret;
	}

	//
	// Serialization and construction
	//
	
	public String toJson() {
		// Trick to avoid backslashing double quotes: use backticks and then replace it at the end 
		String jid = "`id`: `" + this.getId() + "`";
		String jname = "`name`: `" + this.getName() + "`";
		
		String jafterAppend = "`afterAppend`: " + this.afterAppend + "";

		String json = jid + ", " + jname + ", " + jafterAppend;

		return ("{" + json + "}").replace('`', '"');
	}
	public static Schema fromJson(String json) throws DcError {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");

		String name = obj.getString("name");
		if(!Utils.validElementName(name)) {
			throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating schema. ", "Name contains invalid characters. ");
		}

		long afterAppend = obj.has("afterAppend") && !obj.isNull("afterAppend") ? obj.getLong("afterAppend") : -1;

		//
		// Create
		//
		
		Schema schema = new Schema(name);
		schema.afterAppend = afterAppend;
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
			if(!Utils.validElementName(name)) {
				throw new DcError(DcErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name contains invalid characters. ");
			}
		}

		long afterAppend = obj.has("afterAppend") && !obj.isNull("afterAppend") ? obj.getLong("afterAppend") : -1;

		//
		// Update the properties
		//

		if(obj.has("name")) this.setName(obj.getString("name"));
		if(obj.has("afterAppend")) this.afterAppend = obj.getLong("afterAppend");
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
