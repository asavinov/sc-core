package org.conceptoriented.sc.core;

import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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

import org.json.JSONArray;
import org.json.JSONObject;

public class Column {
	private Schema schema;
	public Schema getSchema() {
		return schema;
	}
	
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
	
	private Table input;
	public Table getInput() {
		return input;
	}
	public void setInput(Table table) {
		this.input = table;
	}

	private Table output;
	public Table getOutput() {
		return output;
	}
	public void setOutput(Table table) {
		this.output = table;
	}

	//
	// Data
	//
	
	// Array with output values. Very first element corresponds to the oldest existing id. Very last element corresponds to the newest existing id.
	// Note that some (oldest, in the beginning) elements can be marked for deletion, that is, it is garbage and they are stored with the only purpose to do evaluation by updating the state of other elements 
	private Object[] values; 
	private long length = 0; // It is a physical size allocated for storing output values
	
	//
	// Data access
	//
	
	public Object getValue(long row) {
		return values[(int)row];
	}
	public void setValue(long row, Object value) {
		values[(int)row] = value;
		this.isChanged = true; // Mark column as dirty
	}
	// Convenience method. The first element in the path must be this column. 
	protected Object getValue(List<Column> columns, long row) {
		Object out = row;
		for(Column col : columns) {
			out = col.getValue((long)out);
			if(out == null) break;
		}
		return out;
	}

	public long appendValue(Object value) {
		// Cast the value to type of this column
		if(this.getOutput().getName().equalsIgnoreCase("String")) {
			try { value = value.toString(); } 
			catch (Exception e) { value = ""; }
		}
		else if(this.getOutput().getName().equalsIgnoreCase("Double") || this.getOutput().getName().equalsIgnoreCase("Integer")) {
			if(value instanceof String) {
				try { value = NumberFormat.getInstance(Locale.US).parse(((String)value).trim()); } 
				catch (ParseException e) { value = Double.NaN; }
			}
		}
		
		//
		// Really append (after the last row) and mark as new
		//
		this.length++; // Physical storage
		values[(int)this.newRange.end] = value;
		this.newRange.end++;


		return this.newRange.end-1;
	}

	// They can be deleted either physically immediately or marked for deletion for future physical deletion (after evalution or gargabge collection)
	// We delete only oldest records with lowest ids
	public void remove(long count) { // Remove the oldest records with lowest ids
		// TODO:
	}
	public void remove(Range range) { // Delete the specified range of input ids

	}
	public void remove() { // Delete all input ids
		remove(this.getIdRange());
	}

	//
	// Data dirty state.
	//
	
	// Output value changes (change, set, reset).
	// If some output values has been changed (manually) and hence evaluation of dependent columns might be needed.
	public boolean isChanged = false;



	// Input range changes (additions and deletions).
	// 5,6,7,...,100,...,1000,1001,...
	// [del)[clean)[new)
	// [rowRange) - all records that physically exist and can be accessed including new, up-to-date and deleted

	// Deleted but not evaluated (garbage): Some input elements have been marked for deletion but not deleted yet because the deletion operation needs to be evaluated before physical deletion
	// Records to be deleted after the next re-evaluation
	// Immediately after evaluation these records have to be physically deleted (otherwise the data state will be wrong)
	// Deleted records are supposed to have lowest ids (by assuming that we delete only old records)
	protected Range delRange = new Range();
	public Range getDelRange() {
		return new Range(delRange);
	}

	// Clean (evaluated): Input elements which store up-to-date outputs and hence need not to be evaluated
	// These records have been already evaluated (clean)
	// We need to store start and end rows
	// Alternatively, we can compute this range as full range minus added and deleted
	protected Range cleanRange = new Range();
	public Range getCleanRange() {
		return new Range(cleanRange);
	}

	// Added but not evaluated: Some input elements have been physically added but their output not evaluated (if formula defined) or not set (if output is set manually)
	// Records added but not evaluated yet (dirty). They are supposed to be evaluated in the next iteration.
	// Immediately after evaluation they need to be marked as clean
	// New records have highest ids by assuming that they are newest records
	protected Range newRange = new Range();
	public Range getNewRange() {
		return new Range(newRange);
	}
	


	
	// All currently existing (non-deleted) elements
	public Range getIdRange() {
		return new Range(getCleanRange().start, getNewRange().end);
	}



	// Mark clean records as dirty (new). Deleted range does not change (we cannot undelete them currently). It is a manual way to trigger re-evaluation.
	private void markCleanAsNew() {
		// [del)[clean)[new)
		cleanRange.end = cleanRange.start; // No clean records
		newRange.start = cleanRange.start; // All new range
	}

	// Mark dirty records as clean. It is supposed to be done by evaluation procedure.
	private void markNewAsClean() {
		// [del)[clean)[new)
		cleanRange.end = newRange.end; // All clean records
		newRange.start = newRange.end; // No new range
	}

	// Mark all records as deleted. Note that it is only marking - for real deletion use other methods.
	private void markAllAsDel() {
		// [del)[clean)[new)

		delRange.end = newRange.end;
		
		cleanRange.start = newRange.end;
		cleanRange.end = newRange.end;
		
		newRange.start = newRange.end;
		newRange.end = newRange.end;
	}

	//
	// Formula
	//
	
	protected DcColumnKind kind;
	public DcColumnKind getKind() {
		return this.kind;
	}
	public void setKind(DcColumnKind kind) {
		this.kind = kind;
	}

	public boolean isDerived() {
		DcColumnKind k = this.kind;
		if(this.kind == DcColumnKind.AUTO) {
			k = this.determineAutoColumnKind();
		}

		if(k == DcColumnKind.CALC || k == DcColumnKind.ACCU || k == DcColumnKind.LINK || k == DcColumnKind.CLASS) {
			return true;
		}

		return false;
	}

	protected String formula;
	public String getFormula() {
		return this.formula;
	}
	public void setFormula(String formula) {
		if(this.formula != null && this.formula.equals(formula)) return; // Nothing to change
		this.formula = formula;
		this.setFormulaUpdate(true);
	}
	
	//
	// Accumulation formula
	//
	
	protected String accuformula; // It is applied to accutable
	public String getAccuformula() {
		return this.accuformula;
	}
	public void setAccuformula(String accuformula) {
		if(this.accuformula != null && this.accuformula.equals(accuformula)) return; // Nothing to change
		this.accuformula = accuformula;
		this.setFormulaUpdate(true);
	}
	
	protected String accutable;
	public String getAccutable() {
		return this.accutable;
	}
	public void setAccutable(String accutable) {
		if(this.accutable != null && this.accutable.equals(accutable)) return; // Nothing to change
		this.accutable = accutable;
		this.setFormulaUpdate(true);
	}
	
	protected String accupath; // It leads from accutable to the input table of the column
	public String getAccupath() {
		return this.accupath;
	}
	public void setAccupath(String accupath) {
		if(this.accupath != null && this.accupath.equals(accupath)) return; // Nothing to change
		this.accupath = accupath;
		this.setFormulaUpdate(true);
	}
	
	// Determine if it is syntactically accumulation, that is, it seems to be intended for accumulation.
	// In fact, it can be viewed as a syntactic check of validity and can be called isValidAccumulation
	public boolean isAccumulatedColumnKind() {
		if(this.accuformula == null || this.accuformula.trim().isEmpty()) return false;
		if(this.accutable == null || this.accutable.trim().isEmpty()) return false;
		if(this.accupath == null || this.accupath.trim().isEmpty()) return false;

		if(this.accutable.equalsIgnoreCase(this.getInput().getName())) return false;

		return true;
	}

	// Try to auto determine what is the formula type and hence how it has to be translated and evaluated
	public DcColumnKind determineAutoColumnKind() {

		if((this.formula == null || this.formula.trim().isEmpty()) 
				&& (this.accuformula == null || this.accuformula.trim().isEmpty())
				&& (this.descriptor == null || this.descriptor.trim().isEmpty())) {
			return DcColumnKind.NONE;
		}
		
		if(this.descriptor != null && !this.descriptor.trim().isEmpty()) {
			return DcColumnKind.CLASS;
		}

		if(this.getOutput().isPrimitive()) { // Either calc or accu
			
			if(this.accuformula == null || this.accuformula.trim().isEmpty()) {
				return DcColumnKind.CALC;
			}
			else {
				return DcColumnKind.ACCU;
			}

		}
		else { // Only tuple
			return DcColumnKind.LINK;
		}
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
		return this.formulaUpdate || this.formulaNew || this.formulaDelete;
	}
	public void setFormulaClean() {
		this.formulaUpdate = false;
		this.formulaNew = false;
		this.formulaDelete = false;
	}

	private boolean formulaUpdate = false; // Formula has been changed
	public void setFormulaUpdate(boolean dirty) {
		this.formulaUpdate = dirty;
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
		List<Column> deps = this.getSchema().getParentDependencies(this);
		if(deps == null) return false;

		// We check only direct dependencies by requesting their complete propagated status
		for(Column dep : deps) {
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
	// Translate
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

	public ExprNode mainExpr; // Either primitive or complex (tuple)

	public ExprNode accuExpr; // Additional values collected from a lesser table

	public void translate() {

		if(this.kind == DcColumnKind.AUTO) {
			this.kind = determineAutoColumnKind();
		}
		if(!this.isDerived()) { // User column - no deps
			this.schema.setParentDependencies(this, null);
			return;
		}

		// Dependencies
		List<Column> columns = new ArrayList<Column>();

		//
		// Step 1: Evaluate main formula to initialize the column. If it is empty then we need to init it with default values
		//

		List<Column> mainColumns = null; // Non-evaluatable column independent of the reason
		this.mainExpr = null;

		this.mainExpr = this.translateMain();
		if(this.mainExpr != null) {
			mainColumns = this.mainExpr.getDependencies();
		}

		if(mainColumns != null) columns.addAll(mainColumns);

		//
		// Step 2: Evaluate accu formula to update the column values (in the case of accu formula)
		//
		if(this.kind == DcColumnKind.ACCU) {

			List<Column> accuColumns = null; // Non-evaluatable column independent of the reason
			this.accuExpr = null;

			if(this.determineAutoColumnKind() == DcColumnKind.ACCU) {
				this.accuExpr = this.translateAccu();
				if(this.accuExpr != null) {
					accuColumns = this.accuExpr.getDependencies();
				}
			}
			
			if(accuColumns != null) columns.addAll(accuColumns);
		}


		//
		// Update dependence graph
		//
		this.schema.setParentDependencies(this, columns);
	}

	public ExprNode translateMain() {
		if(this.formula == null || this.formula.isEmpty()) {
			return null;
		}

		ExprNode expr = new ExprNode();

		//
		// Parse: check correct syntax, find all symbols and store them in dependencies
		//
		expr.formula = this.formula;
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
		if(this.accuformula == null || this.accuformula.isEmpty()) {
			return null;
		}

		ExprNode expr = new ExprNode();

		//
		// Parse: check correct syntax, find all symbols and store them in dependencies
		//
		expr.formula = this.accuformula;
		expr.tableName = this.accutable;
		expr.pathName = this.accupath;
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
	// Evaluate
	//

	Instant evaluateTime = Instant.MIN; // Last time the evaluation has been performed (successfully finished)
	public Instant getEvaluateTime() {
		return this.evaluateTime;
	}
	public void setEvaluateTime() {
		this.evaluateTime = Instant.now();
	}
	public Duration durationFomrLastEvaluated() {
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
	
			Range mainRange = this.getNewRange(); // All dirty/new rows
			if(this.formula == null || this.formula.trim().isEmpty()) { // Initialize to default constant
				Object defaultValue; // Depends on the column type
				if(this.getOutput().isPrimitive()) {
					defaultValue = 0.0;
				}
				else {
					defaultValue = null;
				}
				for(long i=mainRange.start; i<mainRange.end; i++) {
					this.setValue(i, defaultValue);
				}
			}
			else if(mainExpr != null) { // Initialize to what formula returns
				for(long i=mainRange.start; i<mainRange.end; i++) {
					mainExpr.evaluate(i);
					this.setValue(i, mainExpr.result);
				}
			}
	
			//
			// Step 2: Evaluate accu formula to update the column values (in the case of accu formula)
			//
			
			if(this.getKind() == DcColumnKind.ACCU) {
				
				Column accuLinkColumn = accuExpr.path.get(0);
	
				Range accuRange = accuLinkColumn.getIdRange(); // We use all existing rows for full re-evaluate
				for(long i=accuRange.start; i<accuRange.end; i++) {
					long g = (Long) accuLinkColumn.getValue(accuExpr.path, i); // Find group element
					accuExpr.evaluate(i);
					this.setValue(g, accuExpr.result);
				}
	
			}

		}

		this.markNewAsClean(); // Mark dirty as clean

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
		
		if(this.formula != null && !this.formula.isEmpty()) {
			return; // If there is formula then descriptor is not used for dependencies
		}

		//
		// Resolve all dependencies
		//
		List<Column> columns = new ArrayList<Column>();

		if(descriptor != null && !descriptor.isEmpty()) {

			columns = getEvaluatorDependencies();

			schema.setParentDependencies(this, columns); // Update dependency graph
			return;
		}
		else {
			schema.setParentDependencies(this, null); // Non-evaluatable column for any reason
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
		List<Column> columns = schema.getParentDependencies(this);
		evaluator.setColumns(columns);
		
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
		Range range = this.getNewRange();
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

		String jfmla = "`formula`: " + JSONObject.valueToString(this.getFormula()) + "";

		String jafor = "`accuformula`: " + JSONObject.valueToString(this.getAccuformula()) + "";
		String jatbl = "`accutable`: " + JSONObject.valueToString(this.getAccutable()) + "";
		String japath = "`accupath`: " + JSONObject.valueToString(this.getAccupath()) + "";

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
		
		this.kind = DcColumnKind.USER;

		this.values = new Object[1000];
	}
}
