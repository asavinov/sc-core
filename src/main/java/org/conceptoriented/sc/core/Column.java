package org.conceptoriented.sc.core;

import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

public class Column {
	private Schema schema;
	public Schema getSchema() {
		return this.schema;
	}
	
	private final UUID id;
	public UUID getId() {
		return this.id;
	}

	private String name;
	public String getName() {
		return this.name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	private Table input;
	public Table getInput() {
		return this.input;
	}
	public void setInput(Table table) {
		this.input = table;
	}

	private Table output;
	public Table getOutput() {
		return this.output;
	}
	public void setOutput(Table table) {
		this.output = table;
	}

	//
	// Data
	//
	protected ColumnData data;
	public ColumnData getData() {
		return this.data;
	}



	//
	// Formula kind
	//
	
	protected DcColumnKind kind;
	public DcColumnKind getKind() {
		return this.kind;
	}
	public void setKind(DcColumnKind kind) {
		this.kind = kind;
	}
	public boolean isDerived() {
		if(this.kind == DcColumnKind.CALC || this.kind == DcColumnKind.ACCU || this.kind == DcColumnKind.LINK || this.kind == DcColumnKind.CLASS) {
			return true;
		}
		return false;
	}

	//
	// Calc formula
	//
	
	protected String calcFormula;
	public String getCalcFormula() {
		return this.calcFormula;
	}
	public void setCalcFormula(String frml) {
		if(this.calcFormula != null && this.calcFormula.equals(frml)) return; // Nothing to change
		this.calcFormula = frml;
		this.setFormulaChange(true);
	}
	
	//
	// Link formula
	//
	
	protected List<Pair<String,String>> linkFormulas = new ArrayList<Pair<String,String>>();
	public List<Pair<String,String>> getLinkFormulas() {
		return this.linkFormulas;
	}
	public void setFormula(List<Pair<String,String>> linkFormulas) {
		this.linkFormulas.clear();;
		this.linkFormulas.addAll(linkFormulas);
		this.setFormulaChange(true);
	}
	
	//
	// Accumulation formula
	//
	
	protected String accuFormula; // It is applied to accutable
	public String getAccuFormula() {
		return this.accuFormula;
	}
	public void setAccuFormula(String accuFrml) {
		if(this.accuFormula != null && this.accuFormula.equals(accuFrml)) return; // Nothing to change
		this.accuFormula = accuFrml;
		this.setFormulaChange(true);
	}
	
	protected String accuTable;
	public String getAccuTable() {
		return this.accuTable;
	}
	public void setAccuTable(String accuTbl) {
		if(this.accuTable != null && this.accuTable.equals(accuTbl)) return; // Nothing to change
		this.accuTable = accuTbl;
		this.setFormulaChange(true);
	}
	
	protected String accuPath; // It leads from accutable to the input table of the column
	public String getAccuPath() {
		return this.accuPath;
	}
	public void setAccuPath(String accupath) {
		if(this.accuPath != null && this.accuPath.equals(accupath)) return; // Nothing to change
		this.accuPath = accupath;
		this.setFormulaChange(true);
	}
	
	// Initialize
	protected String initFormula;
	public String getInitFormula() {
		return this.initFormula;
	}
	public void setInitFormula(String frml) {
		if(this.initFormula != null && this.initFormula.equals(frml)) return; // Nothing to change
		this.initFormula = frml;
		this.setFormulaChange(true);
	}
	
	// Finalize
	protected String finFormula;
	public String getFinFormula() {
		return this.finFormula;
	}
	public void setFinFormula(String frml) {
		if(this.finFormula != null && this.finFormula.equals(frml)) return; // Nothing to change
		this.finFormula = frml;
		this.setFormulaChange(true);
	}

	//
	// Formula dirty status 
	//

	/**
	 * Status of the data defined by this (and only this) column formula: clean (up-to-date) or dirty.
	 * This status is cleaned by evaluating this column and it made dirty by setting (new), resetting (delete) or changing (updating) the formula.
	 * It is an aggregated status for new, deleted or changed formulas.
	 * It is own status of this columns only (not inherited/propagated).
	 */
	public boolean isFormulaDirty() {
		return this.formulaChange || this.formulaNew || this.formulaDelete;
	}
	public void setFormulaClean() {
		this.formulaChange = false;
		this.formulaNew = false;
		this.formulaDelete = false;
	}

	private boolean formulaChange = false; // Formula has been changed
	public void setFormulaChange(boolean dirty) {
		this.formulaChange = dirty;
	}
	private boolean formulaNew = false; // Formula has been added
	public void setFormulaNew(boolean dirty) {
		this.formulaNew = dirty;
	}
	private boolean formulaDelete = false; // Formula has been deleted
	public void setFormulaDelete(boolean dirty) {
		this.formulaDelete = dirty;
	}

	public boolean isDirtyPropagated() { // Own status and status of all preceding columns (propagated)
		if(this.getDependencies() == null) return false;

		// We check only direct dependencies by requesting their complete propagated status
		for(Column dep : this.getDependencies()) {
			if(dep.getStatus() == null || dep.getStatus().code == DcErrorCode.NONE) {
				if(isFormulaDirty() || dep.isDirtyPropagated()) return true; // Check both own status and (recursively) propagated status 
			}
			else { // Error (translation) status is treated as dirty (also propagated errors)
				return true;
			}
		}

		return false; // All dependencies and this column are up-to-date
	}

	//
	// Column (definition) dependencies
	//
	
	// Types of dependencies:
	// - Formula dependencies: 
	//   - this formula is changed/set/reset -> this means that the output have to be re-evaluated (even if dependent functions are the same)
	//     - so essentially this formula change is equivalent to this function output change
	//   - a dependent formula might have been changed -> effectively this is equivalent to changing all outputs of the dependent function (formula change leads to output change) 
	// - Input set dependencies: 
	//   - calc function has to be re-evaluated for its own added inputs, deleted inputs ignored.
	//   - agg function has to be re-evaluated for added or removed agg table inputs
	//   - link function
	// - Output dependencies:
	//   - calc: if dependent col updates its output for some input, then this function has to re-evaluate this same input (using the new new value of dependent column)
	//   - agg:
	//
	// Evaluates results in:
	// - calc: output changes -> all next functions have to take it into account
	// - agg: output changes
	// - link: output changes
	// - append: output changes; if completely re-populated (with emptying) then added/removed; otherwise added. Note that theoretically, we can compute if there was an change (it is possible that no changes)
	
	// Formula (changes) is a mechanism for changing function outputs and/or set population in addition to direct (API) changes/population.
	// We can assume that any formula change (set, reset etc.) leads to output reset and dirty status (need evaluation).
	// Hence this dirty status (resulting from formula) is propagated to other columns by setting dirty status of other columns (induced or inherited).
	// Thus, there are the following mechanisms:
	// - Formula changes -> this column output is dirty (for the whole range)
	// - Formula change -> output table add/delete status is dirty (for append columns)
	// - API set value -> this column output is dirty (for specific input)
	// - API append/remove record -> this table add/delete status is dirty
	
	// Goal: 
	// Our current evaluation() has to correctly work with different statuses:
	// - This and inherited formula changes and errors (during translation - compile-time)
	// - This and inherited formula evaluation errors (an error can arise during evaluation - run-time)
	// - This and inherited column changes (note that changes are supposed to be only for non-formula columns; note also change does not mean that the columns dirty - it is still up-to-date but it makes other columns dirty)
	// - This and inherited column input changes (add/remove). It defines the horizontal scope which has to be propagated from this table (its input columns) to other columns.
	// Goal:
	// - Marking cycles, e.g., using a flag or getting a list of columns in a cycle.
	
	// Final goal:
	// - We need to define several examples with auto-evaluation (also manual) evaluation and event feeds from different sources like kafka.
	//   We want to publish these examples in open source by comparing them with kafka and other stream processing engines.
	//   Scenario 1: using rest api to feed events
	//   Scenario 2: subscribing to kafka topic and auto-evaluate
	//   Scenario 3: subscribing to kafka topic and writing the result to another kafka topic.
	//   Examples: word count, moving average/max/min (we need some domain specific interpretation like average prices for the last 24 hours),
	//   Scenario 4: Batch processing by loading from csv, evaluation, and writing the result back to csv.
	

	/**
	 * All other columns it directly depends on, that is, columns directly used in its formula to compute output
	 */
	protected List<Column> dependencies = new ArrayList<Column>();
	public List<Column> getDependencies() {
		return this.dependencies;
	}
	public void setDependencies(List<Column> deps) {
		resetDependencies();
		this.dependencies.addAll(deps);
	}
	public void resetDependencies() {
		this.dependencies.clear();
	}

	//
	// Translate formula NEW
	// Parse and bind. Generate dependencies. Generate necessary evaluators. Produce new (error) status of translation.
	//
	
	// Calc evaluator
	Evaluator calcEvaluator;

	// Link evaluators
	List<Pair<Column,Evaluator>> linkEvaluators = new ArrayList<Pair<Column,Evaluator>>();

	// Accu evaluators
	Evaluator initEvaluator;
	Evaluator accuEvaluator;
	Evaluator finEvaluator;
	List<Column> accuPathColumns;

	public void translate2() {

		// Reset
		this.calcEvaluator = null;

		this.linkEvaluators.clear();;

		this.initEvaluator = null;
		this.accuEvaluator = null;
		this.finEvaluator = null;

		this.resetDependencies();
		List<Column> columns = new ArrayList<Column>();
		
		Table inputTable = this.getInput();
		Table outputTable = this.getOutput();

		// Translate depending on the formula kind
		if(this.kind == DcColumnKind.CALC) {
			if(this.calcFormula == null || this.calcFormula.isEmpty()) return;

			this.calcEvaluator = new EvaluatorExpr(inputTable);
			this.calcEvaluator.translate(this.calcFormula);
			columns.addAll(this.calcEvaluator.getDependencies());
		}
		else if(this.kind == DcColumnKind.LINK) {
			for(Pair<String,String> mmbr : this.linkFormulas) { // For each tuple member (assignment) create an expression

				// Right hand side
				EvaluatorExpr expr = new EvaluatorExpr(inputTable);
				expr.translate(mmbr.getRight());
				columns.addAll(expr.getDependencies());
				
				// Left hand side (column of the type table)
				Column assignColumn = this.schema.getColumn(outputTable.getName(), mmbr.getLeft());
				this.linkEvaluators.add(Pair.of(assignColumn,expr));
				columns.add(assignColumn);
			}
		}
		else if(this.kind == DcColumnKind.ACCU) {
			if(this.accuFormula == null || this.accuFormula.isEmpty()) return;

			// Initialization
			this.initEvaluator = new EvaluatorExpr(inputTable);
			this.initEvaluator.translate(this.calcFormula);
			columns.addAll(this.initEvaluator.getDependencies());

			// Accu table and link (group) path
			Table accuTable = this.schema.getTable(this.getAccuTable());
			QName accuLinkPath = QName.parse(this.accuPath);
			this.accuPathColumns = accuLinkPath.resolveColumns(accuTable);
			columns.addAll(this.accuPathColumns);

			// Accu expression
			this.accuEvaluator = new EvaluatorExpr(accuTable);
			this.accuEvaluator.translate(this.accuFormula);
			columns.addAll(this.accuEvaluator.getDependencies());

			// Finalization
			this.finEvaluator = new EvaluatorExpr(inputTable);
			this.finEvaluator.translate(this.finFormula);
			columns.addAll(this.finEvaluator.getDependencies());
		}
		else if(this.getKind() == DcColumnKind.CLASS) {
			;
		}

		this.setDependencies(columns);
	}

	//
	// Evaluate column NEW
	//
	public void evaluate2() {
		
		if(this.getKind() == DcColumnKind.CALC) {
			// Evaluate calc expression
			if(this.calcFormula == null || this.calcFormula.trim().isEmpty() || this.calcEvaluator == null) { // Default
				this.evaluateSimpleDefault();
			}
			else {
				this.evaluateSimple(this.calcEvaluator);
			}
		}
		if(this.getKind() == DcColumnKind.LINK) {

			// Prepare for each rhs expression
			List<List<Column>>[] rhsParamPaths = null;
			Object[][] rhsParamValues = null;
			Object[] rhsResults = null;
			
			// Resolve paths into functions for each member




			Table mainTable = this.getInput();
			// Currently we make full scan by re-evaluating all existing input ids
			Range mainRange = this.data.getIdRange();

			for(long i=mainRange.start; i<mainRange.end; i++) {
				
				// Evaluate ALL child rhs expressions by producing an array of their results 
				int mmbrNo = 0;
				for(Pair<Column,Evaluator> mmbr : this.linkEvaluators) {

					List<List<Column>> paramPaths = rhsParamPaths[mmbrNo]; // Functions for this expression parameters
					Object[] paramValues = rhsParamValues[mmbrNo];
					
					// Read parameters


					// Evaluate this expression
					rhsResults[mmbrNo] = mmbr.getRight().evaluate(paramValues);
				}

				// Use rhsResults[childNo] to find/append this tuple of results in the type table (instead of storing) by using lhs[i]=rhs[i]
				Table typeTable = this.getOutput();
				long out = 0;

				// Store the found/appended id in the output of this column
				this.data.setValue(i, out);
			}
			
			
		}
		else if(this.getKind() == DcColumnKind.ACCU) {
			// Evaluate init expression
			if(this.initFormula == null || this.initFormula.trim().isEmpty() || this.initEvaluator == null) { // Default
				this.evaluateSimpleDefault();
			}
			else {
				this.evaluateSimple(this.initEvaluator);
			}
			
			// Evaluate accu expression
			this.evaluateAccu(this.accuEvaluator, accuPathColumns);

			// Evaluate fin expression
			if(this.calcFormula == null || this.calcFormula.trim().isEmpty() || this.finEvaluator == null) { // Default
				; // Do nothing
			}
			else {
				this.evaluateSimple(this.finEvaluator);
			}
		}

		this.data.markNewAsClean(); // Mark dirty as clean

		this.setFormulaClean(); // Mark up-to-date if successful

		this.setEvaluateTime(); // Store the time of evaluation
	}
	
	private void evaluateSimple(Evaluator evalExpr) {
		Table mainTable = this.getInput();
		// Currently we make full scan by re-evaluating all existing input ids
		Range mainRange = this.data.getIdRange();

		// Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading values
		List<List<Column>> paramPaths = this.resolveParameterPaths(mainTable, evalExpr.getParamPaths());
		Object[] paramValues = new Object[paramPaths.size()]; // Will store values for all params
		Object result; // Will be written to output for each input

		for(long i=mainRange.start; i<mainRange.end; i++) {

			// Read all parameter values including this column output
			int paramNo = 0;
			for(List<Column> paramPath : paramPaths) {
				if(paramPath.get(0) == this) {
					paramValues[paramNo] = this.data.getValue(i);
				}
				else {
					paramValues[paramNo] = paramPath.get(0).data.getValue(paramPath, i);
				}
			}
			
			// Evaluate
			result = evalExpr.evaluate(paramValues);

			// Update output
			this.data.setValue(i, result);
		}
	}
	private void evaluateAccu(Evaluator evalExpr, List<Column> accuLinkColumns) {
		Table mainTable = accuLinkColumns.get(0).getInput(); // [ACCU-specific]
		// The optimal approach is to apply negative accu function for removed elements and then positive accu function for added elements
		// Currently we do full re-evaluate by resetting the accu column outputs and then making full scan through all existing facts
		Range mainRange = mainTable.getIdRange();

		// Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading values
		List<List<Column>> paramPaths = this.resolveParameterPaths(mainTable, evalExpr.getParamPaths());
		Object[] paramValues = new Object[paramPaths.size()]; // Will store values for all params
		Object result; // Will be written to output for each input

		for(long i=mainRange.start; i<mainRange.end; i++) {
			// Find group [ACCU-specific]
			long g = (Long) accuLinkColumns.get(0).getData().getValue(accuLinkColumns, i); // Find group element for this fact

			// Read all parameter values including this column output
			int paramNo = 0;
			for(List<Column> paramPath : paramPaths) {
				if(paramPath.get(0) == this) {
					paramValues[paramNo] = this.data.getValue(g); // [ACCU-specific]
				}
				else {
					paramValues[paramNo] = paramPath.get(0).data.getValue(paramPath, i);
				}
			}

			// Evaluate
			result = this.accuEvaluator.evaluate(paramValues);

			// Update output
			this.data.setValue(g, result);
		}
	}
	private void evaluateSimpleDefault() {
		Range mainRange = this.data.getIdRange(); // All dirty/new rows
		Object defaultValue = getDefaultValue();
		for(long i=mainRange.start; i<mainRange.end; i++) {
			this.data.setValue(i, defaultValue);
		}
	}

	// Resolve the specified path names into data (function) objects taking into account a possible special (out) parameter
	private List<List<Column>> resolveParameterPaths(Table pathTable, List<QName> paramPathNames) {
		List<List<Column>> paramPaths = new ArrayList<List<Column>>();
		for(QName n : paramPathNames) {
			if(this.isOutputParameter(n)) {
				paramPaths.add(Arrays.asList(this)); // Single element in path (this column)
			}
			else {
				paramPaths.add(n.resolveColumns(pathTable)); // Multi-segment paths from the iterated table
			}
		}
		return paramPaths;
	}
	private boolean isOutputParameter(QName qname) {
		if(qname.names.size() != 1) return false;
		return isOutputParameter(qname.names.get(0));
	}
	private boolean isOutputParameter(String paramName) {
		if(paramName.equalsIgnoreCase(ExprNode.OUT_VARIABLE_NAME)) {
			return true;
		}
		else if(paramName.equalsIgnoreCase(this.getName())) {
			return true;
		}
		return false;
	}

	private Object getDefaultValue() { // Depends on the column type
		Object defaultValue;
		if(this.getOutput().isPrimitive()) {
			defaultValue = 0.0;
		}
		else {
			defaultValue = null;
		}
		return defaultValue;
	}

	//
	// Translate formula
	// Parse and bind. Generate an evaluator object. Produce new (error) status of translation.
	//
	
	/**
	 * Status of the column translation. 
	 * It includes its own formulas status as well as status inherited from dependencies.
	 */
	public DcError getStatus() {
		DcError err = null;
		if(this.mainExpr != null) {
			ExprNode errorNode = this.mainExpr.getErrorNode();
			if(errorNode != null) {
				err = errorNode.status;
			}
		}
		
		if(err == null && this.accuExpr != null) {
			ExprNode errorNode = this.accuExpr.getErrorNode();
			if(errorNode != null) {
				err = errorNode.status;
			}
		}

		if(err == null) {
			err = new DcError(DcErrorCode.NONE, "", "");
		}
		
		return err;
	}

	public ExprNode mainExpr; // Either primitive or complex (link)

	public ExprNode accuExpr; // Additional values collected from a lesser table

	public void translate() {

		//
		// Reset
		//
		this.resetDependencies();
		this.mainExpr = null;
		this.accuExpr = null;

		//
		// Step 1: Evaluate main formula to initialize the column. If it is empty then we need to init it with default values
		//

		this.mainExpr = this.translateMain();

		//
		// Step 2: Evaluate accu formula to update the column values (in the case of accu formula)
		//
		if(this.kind == DcColumnKind.ACCU) {
			this.accuExpr = this.translateAccu();
		}

		//
		// Dependence graph
		//

		List<Column> columns = new ArrayList<Column>();

		if(this.mainExpr != null) {
			columns.addAll(this.mainExpr.getDependencies());
		}

		if(this.accuExpr != null) {
			columns.addAll(this.accuExpr.getDependencies());
		}

		this.setDependencies(columns);
	}

	public ExprNode translateMain() {
		if(this.calcFormula == null || this.calcFormula.isEmpty()) {
			return null;
		}

		ExprNode expr = new ExprNode();

		//
		// Parse: check correct syntax, find all symbols and store them in dependencies
		//
		expr.formula = this.calcFormula;
		expr.tableName = this.getInput().getName();
		expr.pathName = "";
		expr.name = this.name;
		
		expr.parse();

		//
		// Bind (check if all the symbols can be resolved)
		//
		expr.table = this.getInput();
		expr.column = this;

		expr.bind();
		
		return expr;
	}
		
	public ExprNode translateAccu() {
		if(this.accuFormula == null || this.accuFormula.isEmpty()) {
			return null;
		}

		ExprNode expr = new ExprNode();

		//
		// Parse: check correct syntax, find all symbols and store them in dependencies
		//
		expr.formula = this.accuFormula;
		expr.tableName = this.accuTable;
		expr.pathName = this.accuPath;
		expr.name = this.name;
		
		expr.parse();

		//
		// Bind (check if all the symbols can be resolved)
		//
		expr.column = this;

		expr.bind();
		
		return expr;
	}

	//
	// Evaluate formula. 
	// Use evaluator object and generate new function outputs for all or some inputs
	//

	Instant evaluateTime = Instant.MIN; // Last time the evaluation has been performed (successfully finished)
	public Instant getEvaluateTime() {
		return this.evaluateTime;
	}
	public void setEvaluateTime() {
		this.evaluateTime = Instant.now();
	}
	public Duration durationFromLastEvaluated() {
		return Duration.between(this.evaluateTime, Instant.now());
	}
	
	public void evaluate() {
		
		if(this.getKind() == DcColumnKind.CLASS) {
			;
		}
		else if(this.getKind() == DcColumnKind.CALC || this.getKind() == DcColumnKind.ACCU || this.getKind() == DcColumnKind.LINK) {
			
			//
			// Step 1: Evaluate main formula to initialize the column. If it is empty then we need to init it with default values
			//
	
			Range mainRange = this.data.getIdRange(); // All dirty/new rows

			if(this.calcFormula == null || this.calcFormula.trim().isEmpty()) { // Initialize to default constant (for example, after deletig the formula)
				Object defaultValue; // Depends on the column type
				if(this.getOutput().isPrimitive()) {
					defaultValue = 0.0;
				}
				else {
					defaultValue = null;
				}
				for(long i=mainRange.start; i<mainRange.end; i++) {
					this.data.setValue(i, defaultValue);
				}
			}
			else if(this.mainExpr != null) { // Initialize to what formula returns
				for(long i=mainRange.start; i<mainRange.end; i++) {
					this.mainExpr.evaluate(i);
					this.data.setValue(i, this.mainExpr.result);
				}
			}
	
			//
			// Step 2: Evaluate accu formula to update the column values (in the case of accu formula)
			//
			
			if(this.getKind() == DcColumnKind.ACCU) {
				
				Column accuLinkColumn = accuExpr.path.get(0);
	
				Range accuRange = accuLinkColumn.getData().getIdRange(); // We use all existing rows for full re-evaluate
				for(long i=accuRange.start; i<accuRange.end; i++) {
					long g = (Long) accuLinkColumn.getData().getValue(accuExpr.path, i); // Find group element
					accuExpr.evaluate(i);
					this.data.setValue(g, accuExpr.result);
				}
	
			}

		}

		this.data.markNewAsClean(); // Mark dirty as clean

		this.setFormulaClean(); // Mark up-to-date if successful

		this.setEvaluateTime(); // Store the time of evaluation
	}

	//
	// Descriptor (if column is computed via Java class and not formula)
	//
	
	private String descriptor;
	public String getDescriptor() {
		return descriptor;
	}
	public void setDescriptor(String descriptor) {
		this.descriptor = descriptor;
		
		if(this.calcFormula != null && !this.calcFormula.isEmpty()) {
			return; // If there is formula then descriptor is not used for dependencies
		}

		//
		// Resolve all dependencies
		//
		List<Column> columns = new ArrayList<Column>();

		if(descriptor != null && !descriptor.isEmpty()) {

			columns = getEvaluatorDependencies();

			this.setDependencies(columns); // Update dependency graph
			return;
		}
		else {
			this.resetDependencies(); // Non-evaluatable column for any reason
		}

		// Here we might want to check the validity of the dependency graph (cycles, at least for this column)
	}

	public List<Column> getEvaluatorDependencies() {
		List<Column> columns = new ArrayList<Column>();

		List<QName> deps = new ArrayList<QName>();
		if(descriptor == null || descriptor.isEmpty()) return columns;

		JSONObject jdescr = new JSONObject(descriptor);
		if(jdescr == null || !jdescr.has("dependencies")) return columns;

		JSONArray jdeps = jdescr.getJSONArray("dependencies");

		QNameBuilder qnb = new QNameBuilder();
		for (int i = 0 ; i < jdeps.length(); i++) {
			QName qn = qnb.buildQName(jdeps.getString(i));
			deps.add(qn);
		}

		for(QName dep : deps) {
			Column col = dep.resolveColumn(schema, this.getInput());
			columns.add(col);
		}
		
		return columns;
	}
	public String getEvaluatorClass() {
		if(descriptor == null) return null;
		JSONObject jdescr = new JSONObject(descriptor);
		return jdescr.getString("class");
	}

	protected ScEvaluator evaluator;
	public ScEvaluator setEvaluator() {
		evaluator = null;
		
		String evaluatorClass = getEvaluatorClass(); // Read from the descriptor
		if(evaluatorClass == null) return null;
		
		//
		// Dynamically load the class by using the schema class loader
		//

		ClassLoader classLoader = schema.getClassLoader();
		
		Class clazz=null;
		try {
			clazz = classLoader.loadClass(evaluatorClass);
	    } catch (ClassNotFoundException e) {
	        e.printStackTrace();
	    }
		
		//
		// Create an instance of an evaluator
		//
	    try {
			evaluator = (ScEvaluator) clazz.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
		return evaluator;
	}

	protected void begingEvaluate() {
		//
		// Prepare evaluator instance
		//

		if(evaluator == null) {
			setEvaluator();
		}
		
		if(evaluator == null) return;
		
		// Pass direct references to the required columns so that the evaluator can use them during evaluation. The first element has to be this (output) column
		evaluator.setColumn(this);
		evaluator.setColumns(this.getDependencies());
		
		evaluator.beginEvaluate();
	}

	protected void endEvaluate() {
		evaluator.endEvaluate();
	}

	/**
	 * Evaluate class. 
	 */
	public void evaluateDescriptor() {
		
		if(descriptor == null || descriptor.isEmpty()) return; 
			
		this.begingEvaluate(); // Prepare (evaluator, computational resources etc.)
		
		if(evaluator == null) return;

		// Evaluate for all rows in the (dirty, new) range
		Range range = this.getData().getNewRange();
		for(long i=range.start; i<range.end; i++) {
			evaluator.evaluate(i);
		}

		this.endEvaluate(); // De-initialize (evaluator, computational resources etc.)
	}

	//
	// Serialization and construction
	//

	public String toJson() {
		// Trick to avoid backslashing double quotes: use backticks and then replace it at the end 
		String jid = "`id`: `" + this.getId() + "`";
		String jname = "`name`: `" + this.getName() + "`";
		
		String jinid = "`id`: `" + this.getInput().getId() + "`";
		String jin = "`input`: {" + jinid + "}";

		String joutid = "`id`: `" + this.getOutput().getId() + "`";
		String jout = "`output`: {" + joutid + "}";

		String jstatus = "`status`: " + (this.getStatus() != null ? this.getStatus().toJson() : "undefined");
		String jdirty = "`dirty`: " + (this.isDirtyPropagated() ? "true" : "false"); // We transfer deep dirty

		String jkind = "`kind`:" + this.kind.getValue() + "";

		String jfmla = "`formula`: " + JSONObject.valueToString(this.getCalcFormula()) + "";

		String jafor = "`accuformula`: " + JSONObject.valueToString(this.getAccuFormula()) + "";
		String jatbl = "`accutable`: " + JSONObject.valueToString(this.getAccuTable()) + "";
		String japath = "`accupath`: " + JSONObject.valueToString(this.getAccuPath()) + "";

		//String jdescr = "`descriptor`: " + (this.getDescriptor() != null ? "`"+this.getDescriptor()+"`" : "null");
		String jdescr = "`descriptor`: " + JSONObject.valueToString(this.getDescriptor()) + "";

		String json = jid + ", " + jname + ", " + jin + ", " + jout + ", " + jdirty + ", " + jstatus + ", " + jkind + ", " + jfmla + ", " + jafor + ", " + jatbl + ", " + japath + ", " + jdescr;

		return ("{" + json + "}").replace('`', '"');
	}
	
	@Override
	public String toString() {
		return "[" + getName() + "]: " + input.getName() + " -> " + output.getName();
	}
	
	@Override
	public boolean equals(Object aThat) {
		if (this == aThat) return true;
		if ( !(aThat instanceof Table) ) return false;
		
		Column that = (Column)aThat;
		
		if(!that.getId().toString().equals(id.toString())) return false;
		
		return true;
	}

	public Column(Schema schema, String name, String input, String output) {
		this.schema = schema;
		this.id = UUID.randomUUID();
		this.name = name;
		this.input = schema.getTable(input);
		this.output = schema.getTable(output);
		
		// Formula
		this.kind = DcColumnKind.USER;

		// Data
		this.data = new ColumnData(this);
	}
}
