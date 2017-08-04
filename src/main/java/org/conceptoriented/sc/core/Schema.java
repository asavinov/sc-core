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
	// Schema dependencies
	//
	
	/**
	 * For each column, we store all other columns it directly depends on, that is, columns that it directly uses in its formula
	 */
/*
	protected Map<Column,List<Column>> dependencies = new HashMap<Column,List<Column>>();
	public List<Column> getParentDependencies(Column col) {
		return this.dependencies.get(col);
	}
	public void setParentDependencies(Column col, List<Column> deps) {
		this.dependencies.put(col, deps);
	}
*/

	public List<Column> getChildDependencies(Column col) {
		// Return all columns which point to the specified column as a dependency, that is, which have this column in its deps
		List<Column> res = this.columns.stream().filter(x -> x.getDependencies() != null && x.getDependencies().contains(col)).collect(Collectors.<Column>toList());
		return res;
	}
	public void emptyDependencies() { // Reset. Normally before generating them.
		this.columns.forEach(x -> x.resetDependencies());
	}

	// Return all columns which do not depend on other columns. 
	// They are starting nodes in the dependency graph
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
		
		for(Column col : this.columns) {
			List<Column> deps = col.getDependencies();
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
		
		for(Column col : this.columns) {
		//for(Map.Entry<Column, List<Column>> entry : dependencies.entrySet()) {
			//Column col = entry.getKey();
			List<Column> deps = col.getDependencies();
			if(deps == null) continue; // Non-evaluatable (no formula or error)
			if(previousColumns.contains(col)) continue; // Skip already evaluated columns

			if(previousColumns.containsAll(deps)) { // All deps have to be evaluated (non-dirty)
				res.add(col); 
			}
		}
		
		return res;
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
					List<Column> deps = col.getDependencies();
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
	 * Evaluate all columns of the schema.
	 * 
	 * The order of column evaluation is determined by the dependency graph. Currently dependencies do not involve ranges, that is, the full range of any dependent column must be up-to-date.
	 * The input range for each evaluated column is determined by its own dirty status, that is, the evaluated inputs depend on this column dirty status. 
	 * The dirty status involves involves two components: set population status (added and removed inputs), change status (updated outputs of inputs).
	 * 
	 * Evaluation results in cleaning this column status by computing the necessary outputs. 
	 */
	public void evaluate() {

		List<Column> readyColumns = new ArrayList<Column>(); // Already evaluated
		List<Column> nextColumns = this.getStartingColumns(); // Initialize. First iteration with column with no dependency formulas. 
		while(nextColumns.size() > 0) {
			for(Column col : nextColumns) {
				if(col.getStatus() == null || col.getStatus().code == DcErrorCode.NONE) { // Only columns without problems can be evaluated
					col.evaluate(); // This will update (clean) the dirty status of each individual column
					readyColumns.add(col);
				}
			}
			nextColumns = this.getNextColumns(readyColumns); // Next iteration
		}

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
