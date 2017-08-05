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
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONObject;


// NEW REFACTORING:

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
	
	protected Map<String, String> linkFormula = new HashMap<String, String>();
	public Map<String,String> getLinkFormulas() {
		return this.linkFormula;
	}
	public void setLinkFormula(Map<String,String> linkFormulas) {
		this.linkFormula.clear();
		this.setFormulaChange(true);
		if(linkFormulas == null) return;
		this.linkFormula.putAll(linkFormulas);
	}
	public void resetLinkFormula() {
		this.linkFormula.clear();
		this.setFormulaChange(true);
	}
	DcError tupleTranslateStatus;
	public void setLinkFormula(String frml) { // Convenience method. Parse tuple {...} and fill member assignments
		this.tupleTranslateStatus = null;
		this.linkFormula.clear();

		Map<String,String> mmbrs = new HashMap<String,String>();

		//
		// Check correct enclosure (curly brackets)
		//
		int open = frml.indexOf("{");
		int close = frml.lastIndexOf("}");

		if(open < 0 || close < 0 || open >= close) {
			this.tupleTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "Problem with curly braces. Tuple expression is a list of assignments in curly braces.");
			return;
		}

		String sequence = frml.substring(open+1, close).trim();

		//
		// Build a list of members from comma separated list
		//
		List<String> members = new ArrayList<String>();
		int previousSeparator = -1;
		int level = 0; // Work only on level 0
		for(int i=0; i<sequence.length(); i++) {
			if(sequence.charAt(i) == '{') {
				level++;
			}
			else if(sequence.charAt(i) == '}') {
				level--;
			}
			
			if(level > 0) { // We are in a nested block. More closing parentheses are expected to exit from this block.
				continue;
			}
			else if(level < 0) {
				this.tupleTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "Problem with curly braces. Opening and closing curly braces must match.");
				return;
			}
			
			// Check if it is a member separator
			if(sequence.charAt(i) == ';') {
				members.add(sequence.substring(previousSeparator+1, i));
				previousSeparator = i;
			}
		}
		members.add(sequence.substring(previousSeparator+1, sequence.length()));

		//
		// Create child tuples from members and parse them
		//
		for(String member : members) {
			int eq = member.indexOf("=");
			if(eq < 0) {
				this.tupleTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "No equality sign. Tuple expression is a list of assignments.");
				return;
			}
			String lhs = member.substring(0, eq).trim();
			if(lhs.startsWith("[")) lhs = lhs.substring(1);
			if(lhs.endsWith("]")) lhs = lhs.substring(0,lhs.length()-1);
			String rhs = member.substring(eq+1).trim();

			mmbrs.put(lhs, rhs);
		}

		this.setLinkFormula(mmbrs);
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
	// Formula dirty status (own or inherited)
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
			if(dep.getTranslateError() == null || dep.getTranslateError().code == DcErrorCode.NONE) {
				if(isFormulaDirty() || dep.isDirtyPropagated()) return true; // Check both own status and (recursively) propagated status 
			}
			else { // Error (translation) status is treated as dirty (also propagated errors)
				return true;
			}
		}

		return false; // All dependencies and this column are up-to-date
	}

	//
	// Formula (translate) dependencies
	//
	
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
	// Translation status
	// Translation errors are produced and stored in different objects like many evaluators or local fields (e.g., for links) so the final status is collected
	//

	public DcError getTranslateError() { // Get single (the first) error (there could be many errors detected)
		List<DcError> errors = this.getTranslateErrors();
		if(errors == null || errors.size() == 0) return null;
		return errors.get(0);
	}
	public List<DcError> getTranslateErrors() { // Empty list in the case of no errors
		List<DcError> errors = new ArrayList<DcError>();
		if(this.kind == DcColumnKind.CALC) {
			if(this.calcEvaluator != null) errors.add(this.calcEvaluator.getTranslateError());
		}
		else if(this.kind == DcColumnKind.LINK) {
			if(this.tupleTranslateStatus != null) errors.add(this.tupleTranslateStatus);
			for(Pair<Column,Evaluator> mmbr : this.linkEvaluators) {
				if(mmbr.getRight() != null) errors.add(mmbr.getRight().getTranslateError());
			}
		}
		else if(this.kind == DcColumnKind.ACCU) {
			if(this.initEvaluator != null) errors.add(this.initEvaluator.getTranslateError());
			if(this.accuEvaluator != null) errors.add(this.accuEvaluator.getTranslateError());
			if(this.finEvaluator != null) errors.add(this.accuEvaluator.getTranslateError());
		}

		return null;
	}
	public boolean hasTranslateErrors() { // Is successfully translated and can be used for evaluation
		if(getTranslateErrors().size() == 0) return false;
		else return true;
	}

	//
	// Translate formula
	// Parse (formulas), bind (columns), build (evaluators). Generate dependencies. Produce new (translate) status.
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

	public void translate() {

		// Reset
		this.calcEvaluator = null;

		this.linkEvaluators.clear();

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
			for(Entry<String,String> mmbr : this.linkFormula.entrySet()) { // For each tuple member (assignment) create an expression

				// Right hand side
				EvaluatorExpr expr = new EvaluatorExpr(inputTable);
				expr.translate(mmbr.getValue());
				columns.addAll(expr.getDependencies());
				
				// Left hand side (column of the type table)
				Column assignColumn = this.schema.getColumn(outputTable.getName(), mmbr.getKey());
				this.linkEvaluators.add(Pair.of(assignColumn, expr));
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

			// Accumulation
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
	// Evaluation status
	// Translation errors are produced and stored in different objects like many evaluators or local fields (e.g., for links) so the final status is collected
	//

	public DcError getEvaluateError() { // Get single (the first) error (there could be many errors detected)
		List<DcError> errors = this.getEvaluateErrors();
		if(errors == null || errors.size() == 0) return null;
		return errors.get(0);
	}
	public List<DcError> getEvaluateErrors() { // Empty list in the case of no errors
		List<DcError> errors = new ArrayList<DcError>();
		if(this.kind == DcColumnKind.CALC) {
			if(this.calcEvaluator != null) errors.add(this.calcEvaluator.getEvaluateError());
		}
		else if(this.kind == DcColumnKind.LINK) {
			for(Pair<Column,Evaluator> mmbr : this.linkEvaluators) {
				if(mmbr.getRight() != null) errors.add(mmbr.getRight().getEvaluateError());
			}
		}
		else if(this.kind == DcColumnKind.ACCU) {
			if(this.initEvaluator != null) errors.add(this.initEvaluator.getEvaluateError());
			if(this.accuEvaluator != null) errors.add(this.accuEvaluator.getEvaluateError());
			if(this.finEvaluator != null) errors.add(this.accuEvaluator.getEvaluateError());
		}

		return null;
	}
	public boolean hasEvaluateErrors() { // Is successfully evaluated
		if(getEvaluateErrors().size() == 0) return false;
		else return true;
	}

	//
	// Evaluate column
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
		
		if(this.getKind() == DcColumnKind.CALC) {
			// Evaluate calc expression
			if(this.calcFormula == null || this.calcFormula.trim().isEmpty() || this.calcEvaluator == null) { // Default
				this.evaluateExprDefault();
			}
			else {
				this.evaluateExpr(this.calcEvaluator, null);
			}
		}
		if(this.getKind() == DcColumnKind.LINK) {
			// Link
			this.evaluateLink();
		}
		else if(this.getKind() == DcColumnKind.ACCU) {
			// Initialization
			if(this.initFormula == null || this.initFormula.trim().isEmpty() || this.initEvaluator == null) { // Default
				this.evaluateExprDefault();
			}
			else {
				this.evaluateExpr(this.initEvaluator, null);
			}
			
			// Accumulation
			this.evaluateExpr(this.accuEvaluator, accuPathColumns);

			// Finalization
			if(this.calcFormula == null || this.calcFormula.trim().isEmpty() || this.finEvaluator == null) { // Default
				; // No finalization if not specified
			}
			else {
				this.evaluateExpr(this.finEvaluator, null);
			}
		}

		this.data.markNewAsClean(); // Mark dirty as clean

		this.setFormulaClean(); // Mark up-to-date if successful

		this.setEvaluateTime(); // Store the time of evaluation
	}
	
	private void evaluateExpr(Evaluator eval, List<Column> accuLinkPath) {
		Table mainTable = accuLinkPath == null ? this.getInput() : accuLinkPath.get(0).getInput(); // Loop/scan table

		// ACCU: Currently we do full re-evaluate by resetting the accu column outputs and then making full scan through all existing facts
		// ACCU: The optimal approach is to apply negative accu function for removed elements and then positive accu function for added elements
		Range mainRange = mainTable.getIdRange();

		// Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading values
		List<List<Column>> paramPaths = this.resolveParameterPaths(mainTable, eval.getParamPaths());
		Object[] paramValues = new Object[paramPaths.size()]; // Will store values for all params
		Object result; // Will be written to output for each input

		for(long i=mainRange.start; i<mainRange.end; i++) {
			// Find group [ACCU-specific]
			Long g = accuLinkPath == null ? i : (Long) accuLinkPath.get(0).getData().getValue(accuLinkPath, i);

			// Read all parameter values including this column output
			int paramNo = 0;
			for(List<Column> paramPath : paramPaths) {
				if(paramPath.get(0) == this) {
					paramValues[paramNo] = this.data.getValue(g); // [ACCU-specific] [FIN-specific]
				}
				else {
					paramValues[paramNo] = paramPath.get(0).data.getValue(paramPath, i);
				}
				paramNo++;
			}

			// Evaluate
			result = eval.evaluate(paramValues);

			// Update output
			this.data.setValue(g, result);
		}
	}
	private void evaluateLink() {

		Table typeTable = this.getOutput();

		Table mainTable = this.getInput();
		// Currently we make full scan by re-evaluating all existing input ids
		Range mainRange = this.data.getIdRange();

		// Each item in this lists is for one member expression 
		// We use lists and not map because want to use common index (faster) for access and not key (slower) which is important for frequent accesses in a long loop.
		List< List<List<Column>> > rhsParamPaths = new ArrayList< List<List<Column>> >();
		List< Object[] > rhsParamValues = new ArrayList< Object[] >();
		List< Object > rhsResults = new ArrayList< Object >();
		Record outRecord = new Record(); // All output values for all expressions along with column names (is used by the search)

		// Initialize items of these lists for each member expression
		for(Pair<Column,Evaluator> mmbr : this.linkEvaluators) {
			Evaluator eval = mmbr.getRight();
			int paramCount = eval.getParamPaths().size();

			rhsParamPaths.add( this.resolveParameterPaths(mainTable, eval.getParamPaths()) );
			rhsParamValues.add( new Object[ paramCount ] );
			rhsResults.add( null );
		}

		for(long i=mainRange.start; i<mainRange.end; i++) {
			
			outRecord.fields.clear();
			
			// Evaluate ALL child rhs expressions by producing an array of their results 
			int mmbrNo = 0;
			for(Pair<Column,Evaluator> mmbr : this.linkEvaluators) {

				List<List<Column>> paramPaths = rhsParamPaths.get(mmbrNo);
				Object[] paramValues = rhsParamValues.get(mmbrNo);
				
				// Read all parameter values (assuming that this column output is not used in link columns)
				int paramNo = 0;
				for(List<Column> paramPath : paramPaths) {
					paramValues[paramNo] = paramPath.get(0).data.getValue(paramPath, i);
					paramNo++;
				}

				// Evaluate this member expression
				Object result = mmbr.getRight().evaluate(paramValues);
				rhsResults.set(mmbrNo, result);
				outRecord.set(mmbr.getLeft().getName(), result);
				
				mmbrNo++; // Iterate
			}

			// Find element in the type table which corresponds to these expression results (can be null if not found and not added)
			Object out = typeTable.find(outRecord, true);
			
			// Update output
			this.data.setValue(i, out);
		}

	}

	private void evaluateExprDefault() {
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
/*
	public DcError getStatus_OLD() {
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

	public void translate_OLD() {

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
*/

	//
	// Evaluate formula. 
	// Use evaluator object and generate new function outputs for all or some inputs
	//
/*
	public void evaluate_OLD() {
		
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
*/

	//
	// Descriptor (if column is computed via Java class and not formula)
	//

	private String descriptor;
	public String getDescriptor() {
		return descriptor;
	}
	public void setDescriptor(String descriptor) {
		this.descriptor = descriptor;
		
/*
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
*/
		// Here we might want to check the validity of the dependency graph (cycles, at least for this column)
	}
/*
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
*/

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

		String jstatus = "`status`: " + (this.getTranslateError() != null ? this.getTranslateError().toJson() : "undefined");
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
