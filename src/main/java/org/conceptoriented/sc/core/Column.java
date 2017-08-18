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

// Conclusions:
// - Fix column kinds and their semantics in the sense what expressions are needed and how they are used to organize loop and computing the whole output.
// - Each column kind is known to the user and has its specific class and/or interface.
// - A column instance is defined via the necessary expression(s) which are given either as syntactic formulas or using Java classes. In both cases a descriptor can be used to encode all necessary parameters for the definition of this kind (or even different kinds if it has such a field).
// - Column object has a switch on column kind and store the corresponding class instance as its specification.
// - This class instance (representing definition kind) knows how to organize loops etc. 
//   - ColumnDefCalc, ColumnDefLink, ColumnDefAccu, ...
// - Each of these classes takes expressions as parameters: either formulas or descriptor or Java class or whatever - it is important that it is possible to initialize such an instance according to the kind semantics.
// - Definitions of all kinds must provide common API and information to the column: errors of any kind (translate/evaluate/inherited/cycles etc.)

// Questions:
// - should definitions deal with dirty status? Evaluate loop should understand it in order to propagate new/changed/del status of inputs. But it does not change this status itself.
//   - When a new element is added then the status is changed automatically.
//   - Evaluator should be able to change this status, for example, after updating accu for new and del elements.
// - where formula dirty status is stored?
// - where formulas are stored? formulas are origins and are provided by the user. on the other hand, there could be different syntax for formulas and each requires its evaluator which knows how to parse them.
//   !!! - definition could use only Evaluator API but be unaware of the formulas and how this object is created. So the column or definition could store formulas, while the definition evaluation logic gets instances of Evaluator.
// !! - User provides formula(s) and two two parameters: 1) Column Kind, 2) Expr/Formula types (exp4j, JS, Java etc. - how formulas are converted to expressions - Evaluator class)

// - Formulas for certain kind with certain syntax. Formula dirty status.
// - Logic of producing Evaluator instances from formulas using classes specific to syntax (input is formula and we need to use Java class for this syntax)
// - Logic of evaluation for certain kind (input is Evaluators, independent of formulas and syntax)



// Column Knows (essentially, how to use single expression(s) to compute all outputs of this column according to the kind):
// - switch(column kind)
//   - what class(s) of evaluator object to create for each kind and how to use it, e.g., translate and get dependencies
//   - one or any evaluators specific to each column kind, what to do if empty evaluation 
//   - use this specific evaluator(s): main table, how to loop, get input(s), where and how to store the output,

// Evaluator knows (essentially, single expression for computing output given inputs, so it is an expression provider or single value calculator):
// - unaware of formula kind, how to loop over main table, where values come from etc.
// - unaware of how to change the result (and also how to read values)
// - single formula, its syntactic conventions, how to parse it, e.g., arithmetic expression or string operations or whatever 
// - main table against columns of this table using its columns in expression
// - the main table is not necessarily is a table of this (evaluated) column, e.g., for accu formula
// - knows how to extract dependencies (other columns) from the formula
// - knows how to resolve columns (it is a generic function which can be factored out, e.g., List<Column> resolveAll(List<String> paths)
// - how to build native expression that really transforms input values to one output value

// Use cases:
// Using custom Java class implementing our interface instead of formula. Descriptor for param specification etc.
// Using standard Java method instead of formula. Descriptor.
// Custom link evaluator, e.g., by searching the output or imposing complex predicate (filter)
// Custom accu evaluator, e.g., using conditional update or selecting only certain facts

// Questions:
// - Is it Evaluator or something else?
// - Does it have to implement our Evaluator interface?
// - Is it specific for some formula kind? 


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
	protected String linkFormula;
	public String getLinkFormula() {
		return this.linkFormula;
	}
	public void setLinkFormula(String frml) {
		if(this.linkFormula != null && this.linkFormula.equals(frml)) return; // Nothing to change
		this.linkFormula = frml;
		this.setFormulaChange(true);
	}

	//
	// Accumulation formula
	//
	
	protected String accuFormula; // It is applied to accuTable
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
	
	protected String accuPath; // It leads from accuTable to the input table of the column
	public String getAccuPath() {
		return this.accuPath;
	}
	public void setAccuPath(String accuPath) {
		if(this.accuPath != null && this.accuPath.equals(accuPath)) return; // Nothing to change
		this.accuPath = accuPath;
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

	//
	// Formula (translate) dependencies
	//
	
	/**
	 * All other columns it directly depends on. These columns are directly used in its formula to compute output.
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

	public List<Column> getDependencies(List<Column> cols) { // Get all unique dependencies of the specified columns (expand dependence tree nodes)
		List<Column> ret = new ArrayList<Column>();
		for(Column col : cols) {
			List<Column> deps = col.getDependencies();
			for(Column d : deps) {
				if(!ret.contains(d)) ret.add(d);
			}
		}
		return ret;
	}

	public boolean isStartingColumn() { // True if this column has no dependencies (e.g., constant expression) or is free (user, non-derived) column
		if(!this.isDerived()) {
			return true;
		}
		else if(this.dependencies.isEmpty()) {
			return true;
		}
		else {
			return false;
		}
	}

	public DcError getDependenceError() { // =canEvaluate. Return one error in the dependencies (recursively) including cyclic dependency error
		for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) { // Loop on expansion layers of dependencies
			for(Column dep : deps) {
				if(dep == this) {
					return new DcError(DcErrorCode.DEPENDENCY_CYCLE_ERROR, "Cyclic dependency.", "This column formula depends on itself directly or indirectly.");
				}
				DcError de = dep.getTranslateError();
				if(de != null && de.code != DcErrorCode.NONE) {
					return de;
				}
			}
		}
		return null;
	}
	public DcError getThisOrDependenceError() {
		DcError ret = this.getTranslateError();
		if(ret != null && ret.code != DcErrorCode.NONE) {
			return ret; // Error in this column
		}
		return this.getDependenceError();
	}

	public boolean isDependenceDirty() { // =needEvaluate. True if one of the dependencies (recursively) is dirty (formula change)
		for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) { // Loop on expansion layers of dependencies
			for(Column dep : deps) {
				if(dep == this) {
					return true; // Cyclic dependency is also an error and hence dirty
				}
				DcError de = dep.getTranslateError();
				if(de != null && de.code != DcErrorCode.NONE) {
					return true; // Any error must be treated as dirty status (propagated further down)
				}
				if(dep.isFormulaDirty()) return true;
			}
		}
		return false; // All dependencies are up-to-date
	}
	public boolean isThisOrDependenceDirty() {
		return this.isFormulaDirty() || this.isDependenceDirty();
	}

	//
	// Translation status
	// Translation errors are produced and stored in different objects like many evaluators or local fields (e.g., for links) so the final status is collected
	//
	
	private List<DcError> translateErrors = new ArrayList<DcError>();
	public List<DcError> getTranslateErrors() { // Empty list in the case of no errors
		return this.translateErrors;
	}
	public DcError getTranslateError() { // Get single (the first) error (there could be many errors detected)
		List<DcError> ret = this.getTranslateErrors();
		if(ret == null || ret.size() == 0) return null;
		return ret.get(0);
	}
	public boolean hasTranslateErrors() { // Is successfully translated and can be used for evaluation
		if(this.translateErrors.size() == 0) return false;
		else return true;
	}

	//
	// Translate
	//
	
	ColumnEvaluatorCalc evaluatorCalc;
	ColumnEvaluatorLink evaluatorLink;
	ColumnEvaluatorAccu evaluatorAccu;

	// Generate Evaluator* from ColumnDefinition*
	public void translate() {
		// Reset
		this.translateErrors.clear();;

		this.evaluatorCalc = null;
		this.evaluatorLink = null;
		this.evaluatorAccu = null;

		this.resetDependencies();
		List<Column> columns = new ArrayList<Column>();
		
		Table inputTable = this.getInput();
		Table outputTable = this.getOutput();

		// Translate depending on the formula kind
		if(this.kind == DcColumnKind.CALC) {
			// In future, this object will be stored in the column instead of multiple formulas
			ColumnDefinitionCalc definitionCalc = new ColumnDefinitionCalc(this.calcFormula);

			// Translate by preparing expressions and other objects
			UserDefinedExpression expr = new UdeFormula(definitionCalc.getFormula());
			this.translateErrors.addAll(expr.getErrors());
			if(this.hasTranslateErrors()) return; // Cannot proceed

			// Evaluator
			evaluatorCalc = new ColumnEvaluatorCalc(expr);

			// Collect dependencies
			// TODO:
			// - who returns dependencies as names: Definition, Ude (need to collect from all Udes), Evaluator?
			// - who resolves (column) names: Definition, Ude (need to collect from all Udes), Evaluator, or here?
			// - who returns dependencies as objects?
			// IDEA: ColumnEvaluator* could be created directly from ColumnDefinition* as one of their methods (instead of manually creating all objects here)
			// - the necessary dependencies are then retrieved from the created ColumnEvaluator* rather than from individual expressions and objects 
			columns.addAll(this.resolveParameters(expr.getParamPaths(), inputTable));
		}
		else if(this.kind == DcColumnKind.LINK) {
			// In future, this object will be stored in the column instead of multiple formulas
			ColumnDefinitionLink definitionLink = new ColumnDefinitionLink(this.linkFormula);
			this.translateErrors.addAll(definitionLink.getErrors());
			if(this.hasTranslateErrors()) return; // Cannot proceed

			// Parse tuple and create a collection of assignments
			Map<String,String> mmbrs = definitionLink.translateLinkFormulas();

			// Create column-expression pairs for each assignment
			List<Pair<Column,UserDefinedExpression>> exprs = new ArrayList<Pair<Column,UserDefinedExpression>>();
			for(Entry<String,String> mmbr : mmbrs.entrySet()) { // For each tuple member (assignment) create an expression

				// Right hand side
				UdeFormula expr = new UdeFormula(mmbr.getValue());
				this.translateErrors.addAll(expr.getErrors());
				if(this.hasTranslateErrors()) return; // Cannot proceed

				// Left hand side (column of the type table)
				Column assignColumn = this.schema.getColumn(outputTable.getName(), mmbr.getKey());
				if(assignColumn == null) { // Binding error
					this.translateErrors.add(new DcError(DcErrorCode.BIND_ERROR, "Binding error.", "Cannot find column: " + assignColumn));
					return;
				}

				exprs.add(Pair.of(assignColumn, expr));

				// Dependencies
				columns.addAll(this.resolveParameters(expr.getParamPaths(), inputTable));
				columns.add(assignColumn);
			}

			// Use this list of assignments to create an evaluator
			evaluatorLink = new ColumnEvaluatorLink(exprs);
		}
		else if(this.kind == DcColumnKind.ACCU) {

			// Initialization
			UserDefinedExpression initExpr = new UdeFormula(this.initFormula);
			this.translateErrors.addAll(initExpr.getErrors());
			if(this.hasTranslateErrors()) return; // Cannot proceed
			columns.addAll(this.resolveParameters(initExpr.getParamPaths(), inputTable));

			// Accu table and link (group) path
			Table accuTable = this.schema.getTable(this.getAccuTable());
			if(accuTable == null) { // Binding error
				this.translateErrors.add(new DcError(DcErrorCode.BIND_ERROR, "Binding error.", "Cannot find table: " + accuTable));
				return;
			}
			QName accuLinkPath = QName.parse(this.accuPath);
			List<Column> accuPathColumns = accuLinkPath.resolveColumns(accuTable);
			if(accuPathColumns == null) { // Binding error
				this.translateErrors.add(new DcError(DcErrorCode.BIND_ERROR, "Binding error.", "Cannot find columns: " + this.accuPath));
				return;
			}
			columns.addAll(accuPathColumns);

			// Accumulation
			UserDefinedExpression accuExpr = new UdeFormula(this.accuFormula);
			this.translateErrors.addAll(accuExpr.getErrors());
			if(this.hasTranslateErrors()) return; // Cannot proceed
			columns.addAll(this.resolveParameters(accuExpr.getParamPaths(), accuTable));

			// Finalization
			UserDefinedExpression finExpr = new UdeFormula(this.finFormula);
			this.translateErrors.addAll(finExpr.getErrors());
			if(this.hasTranslateErrors()) return; // Cannot proceed
			columns.addAll(this.resolveParameters(finExpr.getParamPaths(), inputTable));

			// Use these objects to create an evaluator
			evaluatorAccu = new ColumnEvaluatorAccu(initExpr, accuExpr, finExpr, accuPathColumns);
		}
		else if(this.getKind() == DcColumnKind.CLASS) {
			; // TODO: We do not have CLASS - we will use descriptors in place of formulas. CLASS is then an indicator of formula syntax convention. 
		}

		this.setDependencies(columns);
	}

	private List<Column> resolveParameters(List<QName> params, Table mainTable) { // Resolve specified parameter paths by removing duplicates and recognizing reference to this (out) column
		if(mainTable == null) mainTable = this.getInput();
		List<Column> columns = new ArrayList<Column>();
		for(QName param : params) {
			if(this.isOutputParameter(param)) {
				continue; // Do not add to dependencies
			}

			Table table = mainTable;
			for(String name : param.names) {
				Column col = schema.getColumn(table.getName(), name);
				if(col == null) { // Cannot resolve
					this.translateErrors.add(new DcError(DcErrorCode.BIND_ERROR, "Binding error.", "Cannot find column: " + col));
					break;
				}
				
				if(!columns.contains(col)) { 
					columns.add(col);
				}

				table = col.getOutput(); // Next segment will be resolved from the previous column output
			}
		}
		
		return columns;
	}
	private List<List<Column>> resolveParameterPaths(List<QName> params, Table mainTable) { // Resolve the specified path names into data (function) objects taking into account a possible special (out) parameter
		if(mainTable == null) mainTable = this.getInput();
		List<List<Column>> paths = new ArrayList<List<Column>>();
		for(QName param : params) {
			if(this.isOutputParameter(param)) {
				paths.add(Arrays.asList(this)); // Single element in path (this column)
			}
			else {
				paths.add(param.resolveColumns(mainTable)); // Multi-segment paths from the iterated table
			}
		}
		return paths;
	}

	private boolean isOutputParameter(QName qname) {
		if(qname.names.size() != 1) return false;
		return this.isOutputParameter(qname.names.get(0));
	}
	private boolean isOutputParameter(String paramName) {
		if(paramName.equalsIgnoreCase("["+UdeFormula.OUT_VARIABLE_NAME+"]")) {
			return true;
		}
		else if(paramName.equalsIgnoreCase(UdeFormula.OUT_VARIABLE_NAME)) {
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
	// Parse (formulas), bind (columns), build (evaluators). Generate dependencies. Produce new (translate) status.
	//
/*
	// Calc evaluator
	UserDefinedExpression calcEvaluator;

	// Link evaluators
	List<Pair<Column,UserDefinedExpression>> linkEvaluators = new ArrayList<Pair<Column,UserDefinedExpression>>();
	DcError linkTranslateStatus;
	protected Map<String, String> linkMembers = new HashMap<String, String>();

	// Accu evaluators
	UserDefinedExpression initEvaluator;
	UserDefinedExpression accuEvaluator;
	UserDefinedExpression finEvaluator;
	List<Column> accuPathColumns;
*/

/* OLD together wiith old evaluator above
	public void translate_OLD() {

		// Reset
		this.bindError = null;

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

			this.calcEvaluator = new UdeFormula();
			this.calcEvaluator.translate(this.calcFormula);
			columns.addAll(this.resolveParameters(this.calcEvaluator.getParamPaths(), inputTable));
		}
		else if(this.kind == DcColumnKind.LINK) {

			this.translateLinkFormula(this.linkFormula);
			if(this.linkTranslateStatus != null && this.linkTranslateStatus.code != DcErrorCode.NONE) {
				return; // Tuple translation error
			}
			
			for(Entry<String,String> mmbr : this.linkMembers.entrySet()) { // For each tuple member (assignment) create an expression

				// Right hand side
				UdeFormula expr = new UdeFormula();
				expr.translate(mmbr.getValue());
				columns.addAll(this.resolveParameters(expr.getParamPaths(), inputTable));
				
				// Left hand side (column of the type table)
				Column assignColumn = this.schema.getColumn(outputTable.getName(), mmbr.getKey());
				this.linkEvaluators.add(Pair.of(assignColumn, expr));
				columns.add(assignColumn);
			}
		}
		else if(this.kind == DcColumnKind.ACCU) {
			if(this.accuFormula == null || this.accuFormula.isEmpty()) return;

			// Initialization
			this.initEvaluator = new UdeFormula();
			this.initEvaluator.translate(this.initFormula);
			columns.addAll(this.resolveParameters(this.initEvaluator.getParamPaths(), inputTable));

			// Accu table and link (group) path
			Table accuTable = this.schema.getTable(this.getAccuTable());
			QName accuLinkPath = QName.parse(this.accuPath);
			this.accuPathColumns = accuLinkPath.resolveColumns(accuTable);
			columns.addAll(this.accuPathColumns);

			// Accumulation
			this.accuEvaluator = new UdeFormula();
			this.accuEvaluator.translate(this.accuFormula);
			columns.addAll(this.resolveParameters(this.accuEvaluator.getParamPaths(), accuTable));

			// Finalization
			this.finEvaluator = new UdeFormula();
			this.finEvaluator.translate(this.finFormula);
			columns.addAll(this.resolveParameters(this.finEvaluator.getParamPaths(), inputTable));
		}
		else if(this.getKind() == DcColumnKind.CLASS) {
			;
		}

		this.setDependencies(columns);
	}
*/

/* OLD - moved to link definition
	protected void translateLinkFormula(String frml) { // Parse tuple {...} into a list of member assignments and set error
		this.linkTranslateStatus = null;
		this.linkMembers.clear();
		if(frml == null | frml.isEmpty()) return;

		Map<String,String> mmbrs = new HashMap<String,String>();

		//
		// Check correct enclosure (curly brackets)
		//
		int open = frml.indexOf("{");
		int close = frml.lastIndexOf("}");

		if(open < 0 || close < 0 || open >= close) {
			this.linkTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "Problem with curly braces. Tuple expression is a list of assignments in curly braces.");
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
				this.linkTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "Problem with curly braces. Opening and closing curly braces must match.");
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
				this.linkTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "No equality sign. Tuple expression is a list of assignments.");
				return;
			}
			String lhs = member.substring(0, eq).trim();
			if(lhs.startsWith("[")) lhs = lhs.substring(1);
			if(lhs.endsWith("]")) lhs = lhs.substring(0,lhs.length()-1);
			String rhs = member.substring(eq+1).trim();

			mmbrs.put(lhs, rhs);
		}

		this.linkMembers.putAll(mmbrs);
	}
*/

	//
	// Evaluation status
	// Translation errors are produced and stored in different objects like many evaluators or local fields (e.g., for links) so the final status is collected
	//

	private List<DcError> evaluateErrors = new ArrayList<DcError>();
	public List<DcError> getEvaluateErrors() { // Empty list in the case of no errors
		return this.evaluateErrors;
	}
	public DcError getEvaluateError() { // Get single (the first) error (there could be many errors detected)
		if(this.evaluateErrors == null || this.evaluateErrors.size() == 0) return null;
		return this.evaluateErrors.get(0);
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
	
	// GOAL: Use ColumnEvaluator for evaluation instead of old expressions
	public void evaluate() {
		this.evaluateErrors.clear();
		
		if(this.getKind() == DcColumnKind.CALC) {
			this.evaluatorCalc.evaluate();
			this.evaluateErrors.addAll(this.evaluatorCalc.getErrors());
			if(this.hasEvaluateErrors()) return;
		}
		else if(this.getKind() == DcColumnKind.LINK) {
			this.evaluatorLink.evaluate();
			this.evaluateErrors.addAll(this.evaluatorLink.getErrors());
			if(this.hasEvaluateErrors()) return;
		}
		else if(this.getKind() == DcColumnKind.ACCU) {
			this.evaluatorAccu.evaluate();
			this.evaluateErrors.addAll(this.evaluatorAccu.getErrors());
			if(this.hasEvaluateErrors()) return;
		}

		this.data.markNewAsClean(); // Mark dirty as clean

		this.setFormulaClean(); // Mark up-to-date if successful

		this.setEvaluateTime(); // Store the time of evaluation
	}

/* OLD
	public void evaluate_OLD() {
		
		if(this.getKind() == DcColumnKind.CALC) {
			// Evaluate calc expression
			if(this.calcFormula == null || this.calcFormula.trim().isEmpty() || this.calcEvaluator == null) { // Default
				this.evaluateExprDefault();
			}
			else {
				this.evaluateExpr(this.calcEvaluator, null);
			}
		}
		else if(this.getKind() == DcColumnKind.LINK) {
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
	
	private void evaluateExpr(UserDefinedExpression expr, List<Column> accuLinkPath) {
		Table mainTable = accuLinkPath == null ? this.getInput() : accuLinkPath.get(0).getInput(); // Loop/scan table

		// ACCU: Currently we do full re-evaluate by resetting the accu column outputs and then making full scan through all existing facts
		// ACCU: The optimal approach is to apply negative accu function for removed elements and then positive accu function for added elements
		Range mainRange = mainTable.getIdRange();

		// Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading values
		List<List<Column>> paramPaths = this.resolveParameterPaths(expr.getParamPaths(), mainTable);
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
			result = expr.evaluate(paramValues);

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
		for(Pair<Column,UserDefinedExpression> mmbr : this.linkEvaluators) {
			UserDefinedExpression eval = mmbr.getRight();
			int paramCount = eval.getParamPaths().size();

			rhsParamPaths.add( this.resolveParameterPaths(eval.getParamPaths(), mainTable) );
			rhsParamValues.add( new Object[ paramCount ] );
			rhsResults.add( null );
		}

		for(long i=mainRange.start; i<mainRange.end; i++) {
			
			outRecord.fields.clear();
			
			// Evaluate ALL child rhs expressions by producing an array of their results 
			int mmbrNo = 0;
			for(Pair<Column,UserDefinedExpression> mmbr : this.linkEvaluators) {

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
		Object defaultValue = this.getDefaultValue();
		for(long i=mainRange.start; i<mainRange.end; i++) {
			this.data.setValue(i, defaultValue);
		}
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

		String jstatus = "`status`: " + (this.getThisOrDependenceError() != null ? this.getThisOrDependenceError().toJson() : "null");
		String jdirty = "`dirty`: " + (this.isThisOrDependenceDirty() ? "true" : "false"); // We transfer deep dirty including this column

		String jkind = "`kind`:" + this.kind.getValue() + "";

		String jcalc = "`calcFormula`: " + JSONObject.valueToString(this.getCalcFormula()) + "";

		String jlink = "`linkFormula`: " + JSONObject.valueToString(this.getLinkFormula()) + "";

		String jinit = "`initFormula`: " + JSONObject.valueToString(this.getInitFormula()) + "";
		String jaccu = "`accuFormula`: " + JSONObject.valueToString(this.getAccuFormula()) + "";
		String jatbl = "`accuTable`: " + JSONObject.valueToString(this.getAccuTable()) + "";
		String japath = "`accuPath`: " + JSONObject.valueToString(this.getAccuPath()) + "";

		//String jdescr = "`descriptor`: " + (this.getDescriptor() != null ? "`"+this.getDescriptor()+"`" : "null");
		String jdescr = "`descriptor`: " + JSONObject.valueToString(this.getDescriptor()) + "";

		String json = jid + ", " + jname + ", " + jin + ", " + jout + ", " + jdirty + ", " + jstatus + ", " + jkind + ", " + jcalc + ", " + jlink + ", " + jinit + ", " + jaccu + ", " + jatbl + ", " + japath + ", " + jdescr;

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
