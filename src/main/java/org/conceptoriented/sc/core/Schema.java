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
import java.time.Duration;
import java.time.Instant;
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

			col.setCalcFormula(formula);

			col.setAccuFormula(accuformula);
			col.setAccuTable(accutable);
			col.setAccuPath(accupath);

			col.setDescriptor(descr_string);
			
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
			if(!Utils.validElementName(name)) {
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

		if(obj.has("formula")) column.setCalcFormula(formula);

		if(obj.has("accuformula")) column.setAccuFormula(accuformula);
		if(obj.has("accutable")) column.setAccuTable(accutable);
		if(obj.has("accupath")) column.setAccuPath(accupath);

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
	// Dependency graph
	//
	
	protected void resetDependencies() { // Reset. Normally before generating new dependency graph
		this.columns.forEach(x -> x.resetDependencies());
	}
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
	// Translation (parse and bind formulas, prepare for evaluation)
	//
	
	/**
	 * Parse and bind all column formulas in the schema. 
	 * All dependencies are computed and made up-to-date.
	 * The updated result of translation is stored in individual columns.
	 */
	public void translate() {
		// TODO:

		// UI shows two statuses and we need two mechanisms:

		// - canEvaluate - translation errors of this (error, cycle) or previous (warning) columns
		//   - how to propagate canEvaluate/TranslateError/EvalError status?
		//     - translate or evaluate errors? Probably both are important - if there eval error then we cannot eval next columns precisely as translate errors. the difference is that translate errors are static while eval errors appear during the eval process.
		//   - we need to store/serialize it in the column properties for visualization purposes - currently status field in json
		//     - so we can introduce an error property of each column - canEvaluate - is this own error or if absent, previous formula error.
		//     - 

		// - needEvaluate/isUptodate - dirty status of this (formula changed) or inherited (previous formulas changed)
		//   - how to propagate need-eval (dirty/up-to-date) status? 
		//     - Formula change vs data change? Formula -> this and next. Data change (free/starting columns) -> only next change.
		//  - we need to serialize/store it for visualization purposes - current dirty field in json
		//    - so we can introduce a boolean property of each column - needEvaluate/isUptodate/isDirty - it is computed using dependency graph (which has to be up-to-date, that is, normally after translation). Yes, if this formula changed or (inherited) previous formula changed.

		// Problems:
		// - finding/storing/getting inherited errors/status:
		//   - inherited formula dirty/change
		//   - inherited data dirty/change
		//   - inherited translation errors
		//   - inherited evaluation errors
		//   - self-dependence - it is viewed as formula error, it cannot be evaluated and hence all next column inherit this status
		
		// Final goal:
		// - We need to define several examples with auto-evaluation (also manual) evaluation and event feeds from different sources like kafka.
		//   We want to publish these examples in open source by comparing them with kafka and other stream processing engines.
		//   Scenario 1: using rest api to feed events
		//   Scenario 2: subscribing to kafka topic and auto-evaluate
		//   Scenario 3: subscribing to kafka topic and writing the result to another kafka topic.
		//   Examples: word count, moving average/max/min (we need some domain specific interpretation like average prices for the last 24 hours),
		//   Scenario 4: Batch processing by loading from csv, evaluation, and writing the result back to csv.

		
		//
		// Translate individual columns and build dependency graph
		//

		this.resetDependencies(); // Reset
		for(Column col : this.columns) {
			col.translate();
		}
		
		//
		// Propagate translation status (errors) through dependency graph
		// Goal is to see inherited status directly in column status. 
		// Alternatively, the inherited status could be retrieved dynamically, which is good if something changes in previous columns.
		//
/*
		List<Column> readyColumns = new ArrayList<Column>(); // Already evaluated
		List<Column> nextColumns = this.getStartingColumns(); // Initialize. First iteration with column with no dependency formulas. 
		while(nextColumns.size() > 0) {
			for(Column col : nextColumns) {
				if(col.getTranslateError() == null || col.getTranslateError().code == DcErrorCode.NONE) {
					// If there is at least one error in dependencies then mark this as propagated error
					List<Column> deps = col.getDependencies();
					if(deps == null) { 
						readyColumns.add(col);
						continue;
					}
					// If at least one dependency has errors then this column is not suitable for propagation
					Column errCol = deps.stream().filter(x -> x.getTranslateError() != null && x.getTranslateError().code != DcErrorCode.NONE).findAny().orElse(null);
					if(errCol != null) { // Mark this column as having propagated error
						if(errCol.getTranslateError().code == DcErrorCode.PARSE_ERROR || errCol.getTranslateError().code == DcErrorCode.PARSE_PROPAGATION_ERROR)
							;//col.mainExpr.status = new DcError(DcErrorCode.PARSE_PROPAGATION_ERROR, "Propagated parse error.", "Error in the column: '" + errCol.getName() + "'");
						else if(errCol.getTranslateError().code == DcErrorCode.BIND_ERROR || errCol.getTranslateError().code == DcErrorCode.BIND_PROPAGATION_ERROR)
							;//col.mainExpr.status = new DcError(DcErrorCode.BIND_PROPAGATION_ERROR, "Propagated bind error.", "Error in the column: '" + errCol.getName() + "'");
						else if(errCol.getTranslateError().code == DcErrorCode.EVALUATE_ERROR || errCol.getTranslateError().code == DcErrorCode.EVALUATE_PROPAGATION_ERROR)
							;//col.mainExpr.status = new DcError(DcErrorCode.EVALUATE_PROPAGATION_ERROR, "Propagated evaluation error.", "Error in the column: '" + errCol.getName() + "'");
						else
							;//col.mainExpr.status = new DcError(DcErrorCode.GENERAL, "Propagated error.", "Error in the column: '" + errCol.getName() + "'");
					}
				}
				readyColumns.add(col);
			}
			nextColumns = this.getNextColumns(readyColumns); // Next iteration
		}
		
		//
		// Find columns with cyclic dependencies
		//
		for(Column col : this.getColumns()) {
			if(readyColumns.contains(col)) continue;
			
			// If a column has not been covered during propagation then it belongs to a cycle. The cycle itself is not found by this procedure. 

			if(col.getTranslateError() == null || col.getTranslateError().code == DcErrorCode.NONE) {
				//if(col.mainExpr == null) col.mainExpr = new ExprNode(); // Wrong use. Should not happen.
				//col.mainExpr.status = new DcError(DcErrorCode.DEPENDENCY_CYCLE_ERROR, "Cyclic dependency.", "This column formula depends on itself by using other columns which depend on it.");
			}
		}
*/

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

		for(List<Column> cols = this.getStartingColumns(); cols.size() > 0; cols = this.getNextColumnsEvaluatable(cols)) { // Loop on expansion layers of dependencies forward
			for(Column col : cols) {
				if(!col.isDerived()) continue;
				// TODO: Detect also evaluate errors that could have happened before in this same evaluate loop and prevent this column from evaluation
				DcError de = col.getTranslateError();
				if(de == null || de.code == DcErrorCode.NONE) {
					col.evaluate();
				}
			}
		}

/*
		List<Column> readyColumns = new ArrayList<Column>(); // Already evaluated
		List<Column> nextColumns = this.getStartingColumns(); // Initialize. First iteration with column with no dependency formulas. 
		while(nextColumns.size() > 0) {
			for(Column col : nextColumns) {
				if(col.getTranslateError() == null || col.getTranslateError().code == DcErrorCode.NONE) { // Only columns without problems can be evaluated
					col.evaluate(); // This will update (clean) the dirty status of each individual column
					readyColumns.add(col);
				}
			}
			nextColumns = this.getNextColumnsEvaluatable(readyColumns); // Next iteration
		}
*/

		this.setEvaluateTime(); // Store the time of evaluation
	}
	
	/**
	 * Empty (reset de-populate) and populate all tables with elements independent of their dirty status.
	 * The result of population is stored in the state property of each individual table.
	 * 
	 *  Population means adding elements with only their key attributes. Population is the only way add or remove elements of a table (evaluation changes only function outputs). 
	 *  Optionally, non-key attributes can be also generated. We could assume that some key attributes could be computed via functions but it needs to be deeper studied.
	 *  
	 * Each table has a (explicit or implicit) definition of its elements, that is, how its elements are generated or where they come from. 
	 * This definition is used by this procedure. There are the following types of definitions and the ways elements can be generated:
	 * o Product of key attribute domains. The key domains must be populated before this table can be populated. Only non-primitive tables can be used. 
	 *   1) Primitive key domains are either ignored, or computed via a function, or the product fails (not permitted).
	 *   2) If there are no keys then the record-id is a key. Product is not possible (we cannot reference a table via table columns).
	 *   3) Super means only auto-resolution of columns/functions in the parent tables. 
	 * o Filtered product. The filter formula/function must be ready for evaluation, that is, all its dependencies have to be clean (already evaluated). Note that is already formula dependency. There are two cases: inheritance and product.
	 * o Projection (viewed as a filter). In this case, the project formula must be ready for evaluation and there are two dependencies: 
	 *   1) all its formula dependencies have to be clean (already evaluated) precisely as for filters, and 
	 *   2) the referencing sub-table has to be already populated (it works as a basis for the filter). Hence, it cannot depend on this table population status (otherwise we get a cycle).
	 * 
	 * Importantly, both population and evaluation are directed by the dirty state. If the state is up-to-date then these operations will do nothing and there is only one way to completely recompute the state - mark the complete state as dirty.
	 * Essentially, these procedures get the ranges that need to be re-computed for each element (table or column) and then do the work for these ranges only.
	 * For a column, the range is determined by the subset of the inputs. Complete re-evaluation means that all inputs are dirty (and all their outputs are reset to null).
	 * For tables, the range is determined by ... Complete re-population means removing all elements and then generating new elements. 
	 * 
	 * Types of population:
	 * o Full. All tables are emptied and newly populated. It is full reset of the data state of the schema.
	 * o Import. Only tables with external element providers are emptied (and hence this reset is propagated to other tables).
	 * o Internal.
	 * o Export. Only providers which populate external tables are activated. This does not change this schema.
	 * o Append (import, internal, export). Here we only append new elements without emptying the table.
	 * 
	 * Problems and tasks:
	 * o There are two types of operations: population and evaluation. Can they be combined and unified?
	 *   - Solution 1. pop and eval are independent ops. Yet, pop can cause eval (if a column is needed for pop). And eval can cause pop (if table is needed for eval). 
	 * o There are two types of dependencies: table and column. How they can be combined?
	 *   - Solution 1. Dependency graph has elements of two types: tables and column. Tables can be populated and columns can be evaluated.
	 *       However, table nodes can depend on columns and column nodes can depend on tables (e.g., any column depend on its input table).
	 *   - Solution 2. Table dependencies are reduced to column dependencies. 
	 * o Table population can depend on column evaluation status and column evaluation can depend on table population status. We need to somehow combine it.
	 *  
	 * Limited approach. 
	 * Assumption: no product, inheritance, filters and no keys -> only link/import columns can populate tables (append records).
	 * The necessity to populate is determined by the population dirty status/rules. (New/clean/deleted is not population status/rules - it influences evaluation only.)
	 * Population status/rules is a mechanism which determines if a table need to be populated or de-populated.
	 * For example, auto-deletion parameters (age etc.) are an example of population rules.
	 * Also, a table or its import column might define a parameter for regular population.
	 * If it is supported, then population could be done asynchronously.
	 * This mechanism can trigger population and the population procedure will change the dirty state for some elements which is then propagated to the whole schema.
	 *   
	 * There are rules for triggering evaluation, e.g., doing it periodically or immediately after any dirty state.
	 * The evaluation procedure will read the dirty state of the schema and make it up-to-date.
	 * So it is important to distinguish between the mechanism of triggering population/evaluation and the scope of population/evaluation.
	 * For example, once population or evaluation has been triggered, it will do its work according to the parameters described by the dirty state.
	 * In the case of stream processing, population is responsible for inserting all new records while triggering evaluation is performed according to other rules, that is, the data can be in dirty state for some time.
	 * Appending records is then done not explicitly but rather by starting evaluation of import columns but they also are not evaluated explicitly.
	 * Rather, import columns are marked as having dirty input tables (that is, input table has new records).
	 * Only after that evaluation of input columns will really do something.
	 *  
	 * In fact, this simplified model without product/filters/keys relies on only column evaluations and column dependencies.
	 * To make it work with import columns, it is enough to maintain dirty status for virtual input tables of input columns.
	 * Or we need otherwise make it possible evaluation of input columns only when new data is available.
	 * The main change is that now we do not add records to tables explicitly - we develop special columns for that purpose.
	 * For example, we could develop an import column has an input buffer with records and then inserts them into the output table during evaluation.
	 * Another type of an input column could be a connector to an external data source like file or database which reads some data when evaluatino started.
	 * 
	 */
	public void populate() {
		
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
