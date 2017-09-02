package org.conceptoriented.sc.core;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
	// Three formula types
	//
	
	// It is used for all definition types (by default) but every definition has its own expression kind
	public ExpressionKind expressionKind = ExpressionKind.EXP4J;

	// Calc formula
	protected ColumnDefinitionCalc definitionCalc;
	public ColumnDefinitionCalc getDefinitionCalc() {
		return this.definitionCalc;
	}
	public void setDefinitionCalc(ColumnDefinitionCalc definition) {
		this.definitionCalc = definition;
		this.setFormulaChange(true);
	}

	// Link formula
	protected ColumnDefinitionLink definitionLink;
	public ColumnDefinitionLink getDefinitionLink() {
		return this.definitionLink;
	}
	public void setDefinitionLink(ColumnDefinitionLink definition) {
		this.definitionLink = definition;
		this.setFormulaChange(true);
	}

	//
	// Accumulation formula
	//
	protected ColumnDefinitionAccu definitionAccu;
	public ColumnDefinitionAccu getDefinitionAccu() {
		return this.definitionAccu;
	}
	public void setDefinitionAccu(ColumnDefinitionAccu definition) {
		this.definitionAccu = definition;
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
					return new DcError(DcErrorCode.TRANSLATE_PROPAGATION_ERROR, "Error in column " + dep.getName(), "This column formula depends on a column with errors.");
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
				if(de != null && de.code != DcErrorCode.NONE) {					return true; // Any error must be treated as dirty status (propagated further down)
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
	public void setEvaluatorCalc(ColumnEvaluatorCalc eval) { this.evaluatorCalc = eval; this.expressionKind = ExpressionKind.NONE; }
	ColumnEvaluatorLink evaluatorLink;
	public void setEvaluatorLink(ColumnEvaluatorLink eval) { this.evaluatorLink = eval; this.expressionKind = ExpressionKind.NONE; }
	ColumnEvaluatorAccu evaluatorAccu;
	public void setEvaluatorAccu(ColumnEvaluatorAccu eval) { this.evaluatorAccu = eval; this.expressionKind = ExpressionKind.NONE; }

	// Generate Evaluator* from ColumnDefinition*
	// TODO: What if Evaluator* is provided directly without Formulas/Definition?
	// - setEvaluator means direct, setFormula means translation
	public void translate() {

		this.translateErrors.clear();
		this.resetDependencies();

		List<Column> columns = new ArrayList<Column>();
		
		// Translate depending on the formula kind
		if(this.kind == DcColumnKind.CALC) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorCalc = (ColumnEvaluatorCalc) this.definitionCalc.translate(this);
				this.translateErrors.addAll(definitionCalc.getErrors());
			}
			if(this.evaluatorCalc != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorCalc.getDependencies()); // Dependencies
			}
		}
		else if(this.kind == DcColumnKind.LINK) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorLink = (ColumnEvaluatorLink) this.definitionLink.translate(this);
				this.translateErrors.addAll(definitionLink.getErrors());
			}
			if(this.evaluatorLink != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorLink.getDependencies()); // Dependencies
			}
		}
		else if(this.kind == DcColumnKind.ACCU) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorAccu = (ColumnEvaluatorAccu) this.definitionAccu.translate(this);
				this.translateErrors.addAll(definitionAccu.getErrors());
			}
			if(this.evaluatorAccu != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorAccu.getDependencies()); // Dependencies
			}
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

		String jcalc = "`calcFormula`: " + JSONObject.valueToString(this.getDefinitionCalc() == null ? "" : this.getDefinitionCalc().getFormula()) + "";

		String jlink = "`linkFormula`: " + JSONObject.valueToString(this.getDefinitionLink() == null ? "" : this.getDefinitionLink().getFormula()) + "";

		String jinit = "`initFormula`: " + JSONObject.valueToString(this.getDefinitionAccu() == null ? "" : this.getDefinitionAccu().getInitFormula()) + "";
		String jaccu = "`accuFormula`: " + JSONObject.valueToString(this.getDefinitionAccu() == null ? "" : this.getDefinitionAccu().getAccuFormula()) + "";
		String jatbl = "`accuTable`: " + JSONObject.valueToString(this.getDefinitionAccu() == null ? "" : this.getDefinitionAccu().getAccuTable()) + "";
		String japath = "`accuPath`: " + JSONObject.valueToString(this.getDefinitionAccu() == null ? "" : this.getDefinitionAccu().getAccuPath()) + "";

		String json = jid + ", " + jname + ", " + jin + ", " + jout + ", " + jdirty + ", " + jstatus + ", " + jkind + ", " + jcalc + ", " + jlink + ", " + jinit + ", " + jaccu + ", " + jatbl + ", " + japath;

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
		this.expressionKind = ExpressionKind.EXP4J; // By default

		// Data
		this.data = new ColumnData(this);
	}
}
