package org.conceptoriented.sc.core;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User defined expression. It knows how to computing one output value given several input values.
 * Normally it is implemented by a user class programmatically or produced by a translator from some syntactic representation (formula).
 *  
 * An instance of this class provides a method for computing output value given several input values.
 * 
 * The evaluate method is unaware of where the inputs and the output value come from (that is, it is unaware of columns). 
 * However, it is assumed that these input values are read from some column paths for the current input id by the column evaluate procedure.
 * 
 * An external procedure has to configure each instance by specifying all column paths which correspond to the input values as well as output column name corresponding to the output value.
 * These parameters are then used by two purposes. They are used by the translate procedure to manage dependencies and by the column evaluate procedure to read the inputs and write the output.
 * The first parameter is always the current output and the first column path is always an output column. 
 * 
 * Limitations:
 * - It is not possible to vary input ids just because this evaluate interface is unaware of the ids and their semantics. The evaluate method will not get any id - it will get only output values and return an output value.
 * - The evaluate method does not have access to schema elements like columns and hence cannot read/write data directly, append/search records.
 *  
 * Questions/problems:
 * - How to deal with link columns. One solution is to use one evaluator object for one element in the tuple by returning only primitive value.
 * - Is it possible to reuse evaluators in nested manner? For example, if an evaluator implements a library function then it would be interesting to use these functions in other expressions.
 * 
 * Use cases:
 * - Accumulate. Here the first parameter is explicitly used as the current output and then the updated output is returned. Separate evaluators are used for initialization and finalization.
 * - Link. Approach 1: Separate evaluators are used for each member of the tuple. However, their return values are not stored but rather are used to find/append an element by the column evaluator.
 * - Translation of source expression. An instance of this class is returned by a translator from some expression syntax. For each syntax, there is one translator. The paths can be encoded into the source expressions.
 *   The result is a native expression using its native variables.
 * 
 */
public interface UserDefinedExpression {

	/**
	 * For each instance, its parameters are bound to certain column paths (normally primitive).
	 * This information is used by the evaluation procedure to retrieve the values and pass them as parameters to this evaluator. This procedure gets this list, retrieve these path values given the current input, and passes these values to the evaluator.
	 * Each instance has to be bound to certain paths by an external procedure. Typically, it is done by translating a formula.
	 */
	public void setParamPaths(List<String> paths);
	public List<QName> getParamPaths();
	public List<List<Column>> getResolvedParamPaths();
	/**
	 * Each parameter has a description which can be retrieved by means of this method. 
	 * It is not the best approach because these descriptions are language specific.
	 */
	//public List<String> getParamDescriptions();


	public void translate(String formula);
	public List<DcError> getTranslateErrors();

	/**
	 * Compute output value using the provide input values. 
	 * The first parameter is the current output value (or null).
	 * Note that all parameters are output values of different paths for one and the same input id.
	 */
	public Object evaluate(Object[] params, Object out);
	public DcError getEvaluateError();
}

/**
 * 
 */
class UdeFormula implements UserDefinedExpression {

	public static String OUT_VARIABLE_NAME = "out";
	
	public boolean isExp4j() { return true; }
	public boolean isEvalex() { return false; }
	
	// Formula
	protected String formula;

	protected Table table; // For resolution (binding). Formula terms will be resolved relative to this table

	protected boolean isEquality; // The formula is a single parameter without operations

	// Native expressions produced during translation and used during evaluation
	protected net.objecthunter.exp4j.Expression exp4jExpression;
	protected com.udojava.evalex.Expression evalexExpression;

	// Will be filled by parser and then augmented by binder
	protected List<ExprDependency> exprDependencies = new ArrayList<ExprDependency>();
	protected ExprDependency outDependency;

	//
	// EvaluatorExpr interface
	//
	@Override
	public void setParamPaths(List<String> paths) {
		; // TODO
	}
	@Override
	public List<QName> getParamPaths() {
		List<QName> paths = new ArrayList<QName>();
		for(ExprDependency dep : this.exprDependencies) {
			paths.add(dep.qname);
		}
		return paths;
	}
	@Override
	public List<List<Column>> getResolvedParamPaths() {
		List<List<Column>> paths = new ArrayList<List<Column>>();
		for(ExprDependency dep : this.exprDependencies) {
			paths.add(dep.columns);
		}
		return paths;
	}
	@Override
	public void translate(String formula) {
		this.translateError = null;
		this.formula = formula;
		if(this.formula == null || this.formula.isEmpty()) return;

		try {
			this.parse();
		}
		catch(Exception err) {
			if(this.translateError == null) { // Status has not been set by the failed method
				this.translateError = new DcError(DcErrorCode.PARSE_ERROR, "Parse error", "Cannot parse the formula.");
			}
			return;
		}
		if(this.translateError != null) return;

		try {
			this.bind();
		}
		catch(Exception err) {
			if(this.translateError == null) { // Status has not been set by the failed method
				this.translateError = new DcError(DcErrorCode.BIND_ERROR, "Bind error", "Cannot resolve symbols.");
			}
			return;
		}
		if(this.translateError != null) return;

		try {
			this.build();
		}
		catch(Exception err) {
			if(this.translateError == null) { // Status has not been set by the failed method
				this.translateError = new DcError(DcErrorCode.BUILD_ERROR, "Build error", "Cannot build evaluator object.");
			}
			return;
		}
		if(this.translateError != null) return;
	}
	private DcError translateError;
	@Override
	public List<DcError> getTranslateErrors() { // Find first error or null for no errors. Is meaningful only after translation.
		List<DcError> ret = new ArrayList<DcError>();
		if(this.translateError == null || this.translateError.code == DcErrorCode.NONE) {
			return ret;
		}
		ret.add(this.translateError);
		return ret;
	}
	@Override
	public Object evaluate(Object[] params, Object out) {
		this.evaluateError = null;
		
		// Set all parameters in native expressions
		int paramNo = 0;
		for(ExprDependency dep : this.exprDependencies) {
			Object value = params[paramNo];
			if(value == null) value = Double.NaN;
			try {
				if(this.isEquality) {
					; // Do nothing
				}
				else if(this.isExp4j()) {
					this.exp4jExpression.setVariable(dep.paramName, ((Number)value).doubleValue());
				}
				else if(this.isEvalex()) {
					;
				}
			}
			catch(Exception e) {
				this.evaluateError = new DcError(DcErrorCode.EVALUATE_ERROR, "Evaluate error", "Error setting parameter values. " + e.getMessage());
				return null;
			}
			paramNo++;
		}

		// Set out value (if used)
		if(this.outDependency != null) {
			if(out == null) out = Double.NaN;
			try {
				if(this.isEquality) {
					; // Do nothing
				}
				else if(this.isExp4j()) {
					this.exp4jExpression.setVariable(this.outDependency.paramName, ((Number)out).doubleValue());
				}
				else if(this.isEvalex()) {
					;
				}
			}
			catch(Exception e) {
				this.evaluateError = new DcError(DcErrorCode.EVALUATE_ERROR, "Evaluate error", "Error setting parameter values. " + e.getMessage());
				return null;
			}
		}

		// Evaluate native expression
		Object ret = null;
		try {
			if(this.isEquality) {
				ret = params[0]; // Only one param exists for equalities
			}
			else if(this.isExp4j()) {
				ret = this.exp4jExpression.evaluate();
			}
			else if(this.isEvalex()) {
				ret = this.evalexExpression.eval();
			}
		}
		catch(Exception e) {
			this.evaluateError = new DcError(DcErrorCode.EVALUATE_ERROR, "Evaluate error", "Error evaluating expression. " + e.getMessage());
			return null;
		}

		return ret;
	}
	private DcError evaluateError;
	@Override
	public DcError getEvaluateError() { // Find first error or null for no errors. Is meaningful only after evaluation
		if(this.evaluateError == null || this.evaluateError.code == DcErrorCode.NONE) {
			return null;
		}
		return this.evaluateError;
	}

	//
	// Parse
	//

	/**
	 * Parse formulas by possibly building a tree of expressions with primitive expressions in the leaves.
	 * The result of parsing is a list of symbols.
	 */
	protected void parse() {
		if(this.formula == null || this.formula.isEmpty()) return;

		this.exprDependencies.clear();
		this.outDependency = null;

		//
		// Find all occurrences of columns names (in square brackets or otherwise syntactically identified)
		//
		String ex =  "\\[(.*?)\\]";
		//String ex = "[\\[\\]]";
		Pattern p = Pattern.compile(ex,Pattern.DOTALL);
		Matcher matcher = p.matcher(this.formula);

		List<ExprDependency> names = new ArrayList<ExprDependency>();
		while(matcher.find())
		{
			int s = matcher.start();
			int e = matcher.end();
			String name = matcher.group();
			ExprDependency entry = new ExprDependency();
			entry.start = s;
			entry.end = e;
			names.add(entry);
		}
		
		//
		// Create paths by concatenating dot separated column name sequences
		//
		for(int i = 0; i < names.size(); i++) {
			if(i == names.size()-1) { // Last element does not have continuation
				this.exprDependencies.add(names.get(i));
				break;
			}
			
			int thisEnd = names.get(i).end;
			int nextStart = names.get(i+1).start;
			
			if(this.formula.substring(thisEnd, nextStart).trim().equals(".")) { // There is continuation.
				names.get(i+1).start = names.get(i).start; // Attach this name to the next name as a prefix
			}
			else { // No continuation. Ready to copy as path.
				this.exprDependencies.add(names.get(i));
			}
		}

    	//
		// Process the paths
		//
		for(ExprDependency dep : this.exprDependencies) {
			dep.pathName = this.formula.substring(dep.start, dep.end);
			dep.qname = QName.parse(dep.pathName); // TODO: There might be errors here, e.g., wrong characters in names
		}

    	//
		// Detect identity expressions which have a single parameter without operations
		// It is a workaround to solve the problem of non-numeric expressions (used in links) which cannot be evaluated by a native expression library.
		// For equalities, the evaluator will process them separately without using native evaluator.
		//
		if(this.exprDependencies.size() == 1) {
			ExprDependency dep = this.exprDependencies.get(0);
			if(dep.pathName.equals(this.formula.trim())) {
				this.isEquality = true;
			}
			else {
				this.isEquality = false;
			}
		}

		// Detect out parameter and move out of this list to a separate variable
		int outParamNo = 0;
		for(ExprDependency dep : this.exprDependencies) {
			if(this.isOutputParameter(dep.qname)) {
				break;
			}
			outParamNo++;
		}
		if(outParamNo < this.exprDependencies.size()) {
			this.outDependency = this.exprDependencies.get(outParamNo);
			this.exprDependencies.remove(outParamNo);
		}
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
		return false;
	}

	//
	// Bind (resolve all parameters)
	//
	protected void bind() {
		for(ExprDependency dep : this.exprDependencies) {
			dep.columns = dep.qname.resolveColumns(this.table);
			if(dep.columns == null || dep.columns.size() < dep.qname.names.size()) {
				this.translateError = new DcError(DcErrorCode.BIND_ERROR, "Bind error", "Cannot resolve column path " + dep.pathName);
				return;
			}
		}
	}

	//
	// Build (native expression that can be evaluated)
	//

	public void build() {
		// Clean
		this.exp4jExpression = null;
		this.evalexExpression = null;

		// Build the final (native) expression
		if(this.isExp4j()) {
			this.exp4jExpression = this.buildExp4jExpression();
		}
		else if(this.isEvalex()) {
			this.evalexExpression = this.buildEvalexExpression();
		}
	}
	
	// Build exp4j expression
	protected net.objecthunter.exp4j.Expression buildExp4jExpression() {

		String transformedFormula = this.transformFormula();

		//
		// Create a list of variables used in the expression
		//
		Set<String> vars = new HashSet<String>();
		Map<String, Double> vals = new HashMap<String, Double>();
		for(ExprDependency dep : this.exprDependencies) {
			if(dep.paramName == null || dep.paramName.trim().isEmpty()) continue;
			vars.add(dep.paramName);
			vals.put(dep.paramName, 0.0);
		}
		// Set<String> vars = this.primExprDependencies.stream().map(x -> x.paramName).collect(Collectors.toCollection(HashSet::new));
		
		// Add the current output value as a special (reserved) variable
		if(!vars.contains(OUT_VARIABLE_NAME)) vars.add(OUT_VARIABLE_NAME);
		vals.put(OUT_VARIABLE_NAME, 0.0);

		//
		// Create expression object with the transformed formula
		//
		net.objecthunter.exp4j.Expression exp = null;
		try {
			net.objecthunter.exp4j.ExpressionBuilder builder = new net.objecthunter.exp4j.ExpressionBuilder(transformedFormula);
			builder.variables(vars);
			exp = builder.build(); // Here we get parsing exceptions which might need be caught and processed
		}
		catch(Exception e) {
			this.translateError = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", e.getMessage());
			return null;
		}

		//
		// Validate
		//
		exp.setVariables(vals); // Validation requires variables to be set
		net.objecthunter.exp4j.ValidationResult res = exp.validate(); // Boolean argument can be used to ignore unknown variables
		if(!res.isValid()) {
			this.translateError = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", res.getErrors() != null && res.getErrors().size() > 0 ? res.getErrors().get(0) : "");
			return null;
		}

		return exp;
	}

	// Build Evalex expression
	protected com.udojava.evalex.Expression buildEvalexExpression() {

		String transformedFormula = this.transformFormula();

		//
		// Create a list of variables used in the expression
		//
		Set<String> vars = new HashSet<String>();
		Map<String, Double> vals = new HashMap<String, Double>();
		for(ExprDependency dep : this.exprDependencies) {
			if(dep.paramName == null || dep.paramName.trim().isEmpty()) continue;
			vars.add(dep.paramName);
			vals.put(dep.paramName, 0.0);
		}
		// Set<String> vars = this.primExprDependencies.stream().map(x -> x.paramName).collect(Collectors.toCollection(HashSet::new));
		
		// Add the current output value as a special (reserved) variable
		if(!vars.contains(OUT_VARIABLE_NAME)) vars.add(OUT_VARIABLE_NAME);
		vals.put(OUT_VARIABLE_NAME, 0.0);

		//
		// Create expression object with the transformed formula
		//
		final com.udojava.evalex.Expression exp;
		try {
			exp = new com.udojava.evalex.Expression(transformedFormula);
		}
		catch(Exception e) {
			this.translateError = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", e.getMessage());
			return null;
		}

		//
		// Validate
		//
		vars.forEach(x -> exp.setVariable(x, new BigDecimal(1.0)));
    	try {
    		exp.toRPN(); // Generates prefixed representation but can be used to check errors (variables have to be set in order to correctly determine parse errors)
    	}
    	catch(com.udojava.evalex.Expression.ExpressionException ee) {
			this.translateError = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", ee.getMessage());
			return null;
    	}

		return exp;
	}

	// Replace all occurrences of column paths in the formula by variable names from the symbol table
	private String transformFormula() {
		StringBuffer buf = new StringBuffer(this.formula);
		for(int i = this.exprDependencies.size()-1; i >= 0; i--) {
			ExprDependency dep = this.exprDependencies.get(i);
			if(dep.start < 0 || dep.end < 0) continue; // Some dependencies are not from formula (e.g., group path)
			dep.paramName = "__p__"+i;
			buf.replace(dep.start, dep.end, dep.paramName);
		}
		return buf.toString();
	}

	public UdeFormula() {
	}
	public UdeFormula(String formula, Table table) {
		this.formula = formula;
		this.table = table;
		
		this.translate(formula);
	}
}

class ExprDependency {
	public int start;
	public int end;
	public String pathName; // Original param paths
	public String paramName;
	public QName qname; // Parsed param paths
	List<Column> columns; // Resolved param paths
}

/**
 * For test purposes to check if it useful and can be used for situations.
 * Use cases:
 * - Reusable and binding of parameters: I want to implement the logic of computation which can be applied to different inputs, that is, one class can be instantiated and used for different input paths and tables.
 * This can be implemented by the necessary binding for each new instance.
 * - Issue: each instance is hard-bound to certain column paths so if the schema changes then this binding can break.
 *   - Solution: flexibility is reached by name-based dependencies (as opposed to reference-based) but then it is necessary to translation after each change of the schema.
 * - Issue: Path names have to be stored somewhere if we want to work at the level of names.
 *   - Solution: using formulas and descriptors.
 *
 */
class UdeExample implements UserDefinedExpression {
	
	List<List<Column>> inputPaths = new ArrayList<List<Column>>();
	public List<List<Column>> getInputPaths() {
		return this.inputPaths;
	}

	@Override public void setParamPaths(List<String> paths) {}
	@Override public List<QName> getParamPaths() { return null; }
	@Override public List<List<Column>> getResolvedParamPaths() { return null; }
	@Override public void translate(String formula) {}
	@Override public List<DcError> getTranslateErrors() { return null; }
	@Override public Object evaluate(Object[] params, Object out) { return (double)params[0] + 1; }
	@Override public DcError getEvaluateError() { return null; }
	
	public UdeExample(List<List<Column>> inputPaths) { // Binding of parameter
		this.inputPaths.addAll(inputPaths);
	}
}
