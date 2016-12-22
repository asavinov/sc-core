package org.conceptoriented.sc.core;

import java.text.NumberFormat;
import java.text.ParseException;
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
	// Data access
	//
	
	Object[] values;
	long length = 0;
	
	public Object getValue(long row) {
		return values[(int)row];
	}
	public void setValue(long row, Object value) {
		values[(int)row] = value;
	}
	public long appendValue(Object value) {
		// Cast the value to type of this column
		if(this.getOutput().getName().equalsIgnoreCase("String")) {
			value = value.toString();
		}
		else if(this.getOutput().getName().equalsIgnoreCase("Double") || this.getOutput().getName().equalsIgnoreCase("Integer")) {
			if(value instanceof String) {
				try { value = NumberFormat.getInstance(Locale.US).parse((String)value); } 
				catch (ParseException e) { value = Double.NaN; }
			}
		}
		
		long row = length++;
		values[(int)row] = value;
		return row;
	}

	public void removeDelRange(Range delRange) {
		// Currently, do not do anything. The deleted values are still there - they are not supposed to be accessed. The table knows about semantics of these intervals.
	}

	// Convenience method. The first element should be this column. 
	protected Object getValue(List<Column> columns, long row) {
		Object out = row;
		for(Column col : columns) {
			out = col.getValue((long)out);
			if(out == null) break;
		}
		return out;
	}

	//
	// Formula
	//
	
	protected String formula;
	public String getFormula() {
		return this.formula;
	}
	public void setFormula(String formula) {
		if(this.formula != null && this.formula.equals(formula)) return; // Nothing to change
		this.formula = formula;
		this.setDirtyDeep(true);
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
		this.setDirtyDeep(true);
	}
	
	protected String accutable;
	public String getAccutable() {
		return this.accutable;
	}
	public void setAccutable(String accutable) {
		if(this.accutable != null && this.accutable.equals(accutable)) return; // Nothing to change
		this.accutable = accutable;
		this.setDirtyDeep(true);
	}
	
	protected String accupath; // It leads from accutable to the input table of the column
	public String getAccupath() {
		return this.accupath;
	}
	public void setAccupath(String accupath) {
		if(this.accupath != null && this.accupath.equals(accupath)) return; // Nothing to change
		this.accupath = accupath;
		this.setDirtyDeep(true);
	}
	
	// Determine if it is syntactically accumulation, that is, it seems to be intended for accumulation.
	// In fact, it can be viewed as a syntactic check of validity and can be called isValidAccumulation
	public boolean isAccumulation() {
		if(this.accuformula == null || this.accuformula.trim().isEmpty()) return false;
		if(this.accutable == null || this.accutable.trim().isEmpty()) return false;
		if(this.accupath == null || this.accupath.trim().isEmpty()) return false;

		if(this.accutable.equalsIgnoreCase(this.getInput().getName())) return false;

		return true;
	}

	// Try to auto determine what is the formula type and hence how it has to be translated and evaluated
	public DcFormulaType determineAutoFormulaType() {

		if((this.formula == null || this.formula.trim().isEmpty()) && (this.accuformula == null || this.accuformula.trim().isEmpty())) {
			return DcFormulaType.NONE;
		}
		
		if(this.getOutput().isPrimitive()) { // Either calc or accu
			
			if(this.accuformula == null || this.accuformula.trim().isEmpty()) {
				return DcFormulaType.CALC;
			}
			else {
				return DcFormulaType.ACCU;
			}

		}
		else { // Only tuple
			return DcFormulaType.TUPLE;
		}
	}

	//
	// Data status
	//

	/**
	 * Status of the data: clean (up-to-date) or dirty.
	 * Evaluation is the only method that cleans this status in the case of formula columns.
	 * Data change (append, delete, update) or formula change make this status dirty.
	 * Note that here we store own status which propagates through dependencies. 
	 */
	private boolean dirty = false; // Own status
	public boolean isDirty() {
		return this.dirty;
	}
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	public boolean isDirtyDeep() { // Own status and status of all preceding cols (propagated)
		if(this.dirty) { // Own dirty
			return true;
		}

		// TODO: Cycle-unsafe code. All columns in a cycle have to be detected and marked as having formula error so not evaluatable and hence dirty.
		List<Column> deps = this.getSchema().getParentDependencies(this);
		if(deps == null) {
			return false;
		}
		Column dirtyCol = deps.stream().filter(x -> x.isDirtyDeep()).findAny().orElse(null); // Recursion. Find at least one parent column with dirty status
		if(dirtyCol != null) {
			return true; // This column is also dirty
		}

		return false; // All dependencies and this column are up-to-date
	}
	public void setDirtyDeep(boolean dirty) { // Set own status and status of all following cols (propagated)
		this.dirty = dirty;

		if(!dirty) { // Cleaning is not propagated automatically
			return;
		}

		// TODO: Cycle-unsafe code. All columns in a cycle have to be detected and marked as having formula error so not evaluatable and hence dirty.
		List<Column> deps = this.getSchema().getChildDependencies(this);
		deps.forEach(x -> x.setDirtyDeep(dirty)); // Recursion
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

		if(determineAutoFormulaType() == DcFormulaType.NONE) {
			this.schema.setParentDependencies(this, null);
			return;
		}
		
		//
		// Step 1: Evaluate main formula to initialize the column. If it is empty then we need to init it with default values
		//

		List<Column> mainColumns = null; // Non-evaluatable column independent of the reason
		this.mainExpr = null;

		this.mainExpr = this.translateMain();
		if(this.mainExpr != null) {
			mainColumns = this.mainExpr.getDependencies();
		}

		//
		// Step 2: Evaluate accu formula to update the column values (in the case of accu formula)
		//
		
		List<Column> accuColumns = null; // Non-evaluatable column independent of the reason
		this.accuExpr = null;

		if(this.determineAutoFormulaType() == DcFormulaType.ACCU) {
			this.accuExpr = this.translateAccu();
			if(this.accuExpr != null) {
				accuColumns = this.accuExpr.getDependencies();
			}
		}

		//
		// Update dependence graph
		//
		List<Column> columns = mainColumns;
		if(accuColumns != null) {
			if(columns == null) columns = accuColumns;
			else columns.addAll(accuColumns);
		}
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

	public void evaluate() {
		
		if(determineAutoFormulaType() == DcFormulaType.NONE) {
			this.setDirty(false);
			return;
		}
		
		this.setDirtyDeep(true); // Invalidate data

		//
		// Step 1: Evaluate main formula to initialize the column. If it is empty then we need to init it with default values
		//

		Range mainRange = this.getInput().getNewRange(); // All dirty/new rows
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
		
		if(this.determineAutoFormulaType() == DcFormulaType.ACCU) {
			Range accuRange = accuExpr.table.getNewRange(); // All dirty/new rows
			for(long i=accuRange.start; i<accuRange.end; i++) {
				long g = (Long) accuExpr.path.get(0).getValue(accuExpr.path, i); // Find group element
				accuExpr.evaluate(i);
				this.setValue(g, accuExpr.result);
			}
		}

		this.setDirty(false); // Validate own data (make up-to-date) if success
	}

	//
	// Descriptor
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
		Range range = input.getNewRange();
		for(long i=range.start; i<range.end; i++) {
			evaluator.evaluate(i);
		}

		this.endEvaluate(); // De-initialize (evaluator, computational resources etc.)
	}

	public String toJson() {
		// Trick to avoid backslashing double quotes: use backticks and then replace it at the end 
		String jid = "`id`: `" + this.getId() + "`";
		String jname = "`name`: `" + this.getName() + "`";
		
		String jinid = "`id`: `" + this.getInput().getId() + "`";
		String jin = "`input`: {" + jinid + "}";

		String joutid = "`id`: `" + this.getOutput().getId() + "`";
		String jout = "`output`: {" + joutid + "}";

		String jstatus = "`status`: " + (this.getStatus() != null ? this.getStatus().toJson() : "undefined");
		String jdirty = "`dirty`: " + (this.isDirtyDeep() ? "true" : "false"); // We transfer deep dirty

		String jfmla = "`formula`: " + JSONObject.valueToString(this.getFormula()) + "";

		String jafor = "`accuformula`: " + JSONObject.valueToString(this.getAccuformula()) + "";
		String jatbl = "`accutable`: " + JSONObject.valueToString(this.getAccutable()) + "";
		String japath = "`accupath`: " + JSONObject.valueToString(this.getAccupath()) + "";

		//String jdescr = "`descriptor`: " + (this.getDescriptor() != null ? "`"+this.getDescriptor()+"`" : "null");
		String jdescr = "`descriptor`: " + JSONObject.valueToString(this.getDescriptor()) + "";

		String json = jid + ", " + jname + ", " + jin + ", " + jout + ", " + jdirty + ", " + jstatus + ", " + jfmla + ", " + jafor + ", " + jatbl + ", " + japath + ", " + jdescr;

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
		
		values = new Object[1000];
	}
}


enum DcFormulaType {
	NONE(0), // No formula
	AUTO(10), // Auto. Needs to be determined.
	UNKNOWN(20), // Cannot determine
	CALC(30), // Calculated (row-based, no accumulation, non-complex)
	TUPLE(40), // Tuple (complex)
	ACCU(50), // Accumulation
	;

	private int value;

	public int getValue() {
		return value;
	}

	private DcFormulaType(int value) {
		this.value = value;
	}
}
