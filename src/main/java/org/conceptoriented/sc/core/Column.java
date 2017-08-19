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

// NEXT:
//- !!! Example/test with programmatically defined custom ColumnEvaluator (without formula) with Java class as UDE -> anology with MR (map, reduce etc. are like ColumnEvaluatorCalc (map), ColumnEvaluatorAccu (reduce), ColumnEvaluatorLink (join))
//  - Custom link evaluator, e.g., by searching the output or imposing complex predicate (filter)
//  - Custom accu evaluator, e.g., using conditional update or selecting only certain facts

// - constant UDE (easy to compute but not necessary)
// - equal to column UDE (easier to compute but not necessary)
// - How to deal with empty formulas and defaults in a principled manner?
// - Issue: multiple [out] occurrences in a formula (currently only one is possible)
// - Use ColumnDefinition to store formulas instead of individual strings -> Use translation directly from ColumnDefinition* to ColumnEvaluator*
//

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

	protected Object getDefaultValue() { // Depends on the column type. Maybe move to Data class
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

	private List<Column> getDependencies(UserDefinedExpression expr) { // Get parameter paths from expression and extract (unique) column from them
		List<Column> columns = new ArrayList<Column>();
		
		List<List<Column>> paths = expr.getResolvedParamPaths();
		for(List<Column> path : paths) {
			for(Column col : path) {
				if(!this.dependencies.contains(col) && !columns.contains(col)) {
					columns.add(col);
				}
			}
		}
		return columns;
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
		this.translateErrors.clear();

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
			UserDefinedExpression expr = new UdeJava(definitionCalc.getFormula(), inputTable);
			this.translateErrors.addAll(expr.getTranslateErrors());
			if(this.hasTranslateErrors()) return; // Cannot proceed

			// Evaluator
			evaluatorCalc = new ColumnEvaluatorCalc(this, expr);

			columns.addAll(this.getDependencies(expr));
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
				UdeJava expr = new UdeJava(mmbr.getValue(), inputTable);
				this.translateErrors.addAll(expr.getTranslateErrors());
				if(this.hasTranslateErrors()) return; // Cannot proceed

				// Left hand side (column of the type table)
				Column assignColumn = this.schema.getColumn(outputTable.getName(), mmbr.getKey());
				if(assignColumn == null) { // Binding error
					this.translateErrors.add(new DcError(DcErrorCode.BIND_ERROR, "Binding error.", "Cannot find column: " + assignColumn));
					return;
				}

				exprs.add(Pair.of(assignColumn, expr));

				// Dependencies
				columns.addAll(this.getDependencies(expr));
				columns.add(assignColumn);
			}

			// Use this list of assignments to create an evaluator
			evaluatorLink = new ColumnEvaluatorLink(this, exprs);
		}
		else if(this.kind == DcColumnKind.ACCU) {

			// Initialization (always initialize - even for empty formula)
			UserDefinedExpression initExpr = null;
			if(this.finFormula == null || this.finFormula.isEmpty()) { // TODO: We need UDE for constants and for equality (equal to the specified column)
				initExpr = new UdeJava(this.getDefaultValue().toString(), inputTable);
			}
			else {
				initExpr = new UdeJava(this.finFormula, inputTable);
			}
			this.translateErrors.addAll(initExpr.getTranslateErrors());
			if(this.hasTranslateErrors()) return; // Cannot proceed
			columns.addAll(this.getDependencies(initExpr));

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
			UserDefinedExpression accuExpr = new UdeJava(this.accuFormula, accuTable);
			this.translateErrors.addAll(accuExpr.getTranslateErrors());
			if(this.hasTranslateErrors()) return; // Cannot proceed
			columns.addAll(this.getDependencies(accuExpr));

			// Finalization
			UserDefinedExpression finExpr = null;
			if(this.finFormula != null && !this.finFormula.isEmpty()) {
				finExpr = new UdeJava(this.finFormula, inputTable);
				this.translateErrors.addAll(finExpr.getTranslateErrors());
				if(this.hasTranslateErrors()) return; // Cannot proceed
				columns.addAll(this.getDependencies(finExpr));
			}

			// Use these objects to create an evaluator
			evaluatorAccu = new ColumnEvaluatorAccu(this, initExpr, accuExpr, finExpr, accuPathColumns);
		}
		else if(this.getKind() == DcColumnKind.CLASS) {
			; // TODO: We do not have CLASS - we will use descriptors in place of formulas. CLASS is then an indicator of formula syntax convention. 
		}

		this.setDependencies(columns);
	}

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
