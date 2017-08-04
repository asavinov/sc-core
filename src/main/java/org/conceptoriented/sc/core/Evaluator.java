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
 *   The result is a native expression using its native variables as well as a list of bindings from column paths to these native variables.
 * 
 */
public interface Evaluator {

	/**
	 * For each instance, its parameters are bound to certain column paths (normally primitive).
	 * This information is used by the evaluation procedure to retrieve the values and pass them as parameters to this evaluator. This procedure gets this list, retrieve these path values given the current input, and passes these values to the evaluator.
	 * Each instance has to be bound to certain paths by an external procedure. Typically, it is done by translating a formula.
	 */
	public void setParamPaths(List<String> paths);
	public List<QName> getParamPaths();
	public List<Column> getDependencies();
	/**
	 * Each parameter has a description which can be retrieved by means of this method. 
	 * It is not the best approach because these descriptions are language specific.
	 */
	//public List<String> getParamDescriptions();


	public void translate(String formula);

	/**
	 * Compute output value using the provide input values. 
	 * The first parameter is the current output value (or null).
	 * Note that all parameters are output values of different paths for one and the same input id.
	 */
	public Object evaluate(Object[] params);
}



/**
 * There are two major approaches to user-defined functions which differ in what parameters they get and use to get data:
 * 
 * 
 * 2) An evaluator instance gets a list of column objects (column or column data or whatever like column id) which are resolved by the system according to this class or instance declaration. 
 * This declaration of the necessary columns can be provided in meta-information of the class like descriptor, by the static methods of the class, or by the instance itself.
 * The evaluator then uses these columns to access the necessary data. Normally, it gets also some input id for which it has to produce computations. 
 * And it should know also the column object it belongs to. As a consequence, this class must know how to read/write values using functions, that is, it has to know this API (like column data).
 * One advantage of this approach is that the function knows about input ids and can vary them by essentially retrieving arbitrary data. For example, it is needed for time series analysis like moving average.
 * Another property is that theoretically, the evaluator can find and use the necessary columns itself, for example, during the initialization of the instance. However, explicit declaration is needed for dependency management.
 * API Design. The class must know data API (at least read output given input). It also must know how to deal with input ids, particularly, validity ranges.
 * The evaluator gets only input id as parameter (not column output values) and uses then column references to read the necessary values.
 * Do we need to work with subsets/groups by using inverse functions? Conceptually not because this means physically producing a new collection. 
 * How to implement link/append columns? To search/append or not to search.
 * - !!! One option is that it returns a list of (expression) expression outputs so essentially it is a list of normal expressions. And then the system does search and append. This system knows that it is a link function and it will return a list of values for certain columns (as declared) to be searched/appended. A column evaluator in this case is defined as a list of normal columns which can be calculated or link columns, and they can be evaluated individually.
 *   Note that in this case, the system has to write outputs itself (for all functions), that is, evaluators cannot change columns.
 * - Another option is that functions are able to inverse search/append by returning id (rather than list of values). The system uses this function precisely as any other function by simply storing its output (or the function itself stores the found id).
 *  
 * Possible principles:
 * - evaluators can only read and not write
 * - the system gets the output or a list of outputs (for link evaluators) and then writes it or resolved/appended id to the function.
 * - for accu evaluators, an additional parameter is passed and the evaluator has to know how to use it. In fact, it is the only difference from normal evaluators so we always can assume the existence of this parameter (and the system can even always pass it even for calc columns).
 * 
 * Tasks and alternatives:
 * - iterating through input ids, for example, for moving average or for searching. This requires having a reference to a function object like ColumnData.
 * - if the algorithm uses arbitrary input ids then it depends on the corresponding range, so this dependency has to be taken into account, for example, the class could declare that it will use ALL range or previous N range or whatever.
 * - inverse functions for getting input(s) given output(s) might be needed for link columns or for look ups and search.
 * - updating function: by the function or by the system? If by the function then it breaks the conception somewhat if it can update many outputs. So initially we could prohibit updates. In this case, we need to split API into reading and writing. Note that read-only functions can be optimized, for example, a (in-memory) copy could be created with the necessary range or otherwise optimized for this function.
 * - updating function for many inputs. We do not have a use case for this (new values could overwrite previous outputs) but if it is possible then the system might need to know this range for dependency management. Also, we could prohibit from writing/updating outputs.
 * - appending new records might be needed for link/append columns: by the function or by the system?
 * - reuse of an evaluator implementation (formula) for different columns declaratively without hard-coding column names.
 *   For example, we implemented some complex algorithm and want to provide it as a plug-in where the user can apply it to different columns.
 *   The evaluator has to use its interval/local parameters while column references are specified declaratively and resolved at initialization.
 *   Note that the column parameters could be different paths and not only simple columns. For the algorithm on the other hand it is important to retrieve values and maybe iterative through input ids (like previous or next).
 *   So we need to introduce the notion of local columns and then bind them to externally provide columns with real data.
 * 
 * 
 * OLD:
 * A user-defined evaluator knows input columns by name by declaring its dependence on these input columns (and their types).
 * Therefore, each such evaluator class is intended for certain input column names only because these names are encoded into it as dependencies.
 * Thus each class cannot be reused for other columns. It is a drawback because we cannot develop generic functions which could be applied to arbitrary columns.
 * Note that the output column name is not encoded into the class.
 * One possible solution to this problem is to use additional specification along with each user defined evaluator. This specification provides dependencies and essentially binds one instance to specific columns by name.
 * In this case, a user defined evaluator implements a generic function for certain input column types but it can be applied to different columns in a schema and this information is provided in the evaluator descriptor.
 * A descriptor (dependencies) could be hard-coded or provided as a separate file, e.g., json file with the same name as the class.
 * Alternatively, dependencies (descriptor) could be provided programmatically for each column like setDependencies. An evaluator class then implements its functions in terms of abstract column numbers.
 * A user-defined evaluator is developed as name-independent function which can be bound to different columns by name using a separate descriptor provided to the column.
 * The descriptor is used for dependency management (computing the graph) and for providing direct column data to the evaluator by using integer column identifiers.
 * An advantage is that evaluators are then reusable functions, for example, we could develop a generic evaluator for arithmetic operations.
 * 
 * The next step would be to allow for nested evaluators. This can be implemented by explicitly creating columns but this means materialization of intermediate results.
 * But what if an evaluator wants to reuse another evaluator by applying its function to the current inputs?
 * We need to take into account that any including nested evaluators can access also other records in their input columns.
 * Also, they could require some additional input columns the parent evaluator does not have. The dependency manager has to know about such uses.
 * One approach to declare in the descriptor that this (parent) evaluator will consume data from a child evaluator. 
 * And the child evaluator will consume some columns (by name). 
 * If the parent is consuming some value from a child evaluator then it is equivalent to consuming a value from a column.
 * In this case however, the parent cannot move along the evaluator by reading inputs - it can only compute one single value.
 * Alternatively, a parent could change inputs (if they are values) and evaluate the child many times.
 * Note that evaluator can change integer row identifiers and retrieve arbitrary outputs from a column where a column is viewed as a function.
 * Probably, the same could be done with child evaluators: set inputs and compute output (instead of retrieving it).
 * In fact, we have an alternative: either a function gets input values or it gets inputs of functions with the functions themselves (so that the inputs can be varied).
 * So a (child) evaluator gets either input values or functions with current inputs.
 * 
 * - Evaluator does not deal with and does not rely on column names (of course, it can get them but it is illegal)
 * - Evaluator uses column objects as (primitive or material) functions which return output for certain input (maybe we need an interface for that purpose which is similar to generic functions/evaluator concept)
 * - Since evaluator uses functions, it has to be able to get validity ranges for them by retrieving various ranges like new, old, or all (so maybe it could be part of generic function/evaluator interface)
 * - Evaluator can vary inputs of the functions it needs, for example, to compute moving average or to shift them. So it can generate new (local) iterations and loops similar to an external driver like the central evaluator which provides the main loop.  
 * - Generic evaluator/function has two alternative interpretations: computing single output for certain input values provided from outside, and computing a range of outputs for a range of inputs provided from outside. 
 */
interface EvaluatorComplex {

	public void setColumn(Column column);

	// The system will pass direct references to all the necessary column objects.
	// This method can be called at any time but we can assume that it will be called when a column is created and its function plug-in instantiated.
	// These columns object have to be stored so that they can be used in the evaluation method to access data in these columns.
	public void setColumns(List<Column> columns);

	//
	// Table loop methods will be called once for each record loop
	// It could be useful for initializing internal (static) variables
	// Also, the function could prepare some objects, for example, resolve names or get direct references/pointers to objects which normally may change between evaluation cycles. 
	//
	public void beginEvaluate();

	//
	// Value/record methods will be called once for each evaluation 
	//
	public void evaluate(long row);
	// It will be called from each (dirty) input of the table
	// Here we need to access:
	// 'this' value (long)
	// column data object reference for all columns we need and declared: object ref
	//   these object references have data access API: column.getValue(input) where input can be 'this' or output of other getValue
	//   this includes this column data object so that we can access our own column values: thisColumn.getValue(offset)
	// output/type table is needed if we want to push records into output
	// input table might be needed just to know more about this column
	
	// So the main object we want to use is a column reference which provides access to data
	// How we use it? And how we reference these column objects from the evaluation method? Indeed, there are many columns we want to use. 
	// One way is to use table-column names: Schema.getColumn("My Table", "My Column").getValue(rowid)

	// Yet, we do not want to resolve names for each access. So we want to store direct references
	// Column col1 = Schema.getColumn("My Table", "My Column");
	// Object val1 = col1.getValue(rowid);
	
	// We want to use many columns. There references can be stored in two ways:
	// - In a dictionary or list, which means that we introduce a new referencing system: names or index.
	//   Columns are then accessed, for example, in this way: dict["Amount"].getValue() or list[25].getValue()
	// - In local variables, which means that we have own custom variables and must assign them. 
	//   For example, this class could have variables: col1, col2, amountColumn etc.
	// The best way is that the class decides itself but it has a mechanism for assigning these references. 
	// For example, these column references could be assigned in beginEvaluate or in the constructor. 


	
	
	// We can do row id arithmetics by having access to valid range (but we cannot change these ranges - they will be changed by the driver)
	// It can be needed for rolling functions
	//Range range1 = thisColumn.added; // Dirty, new, to be legally added after they are computed
	//Range range2 = thisColumn.rows; // Clean, have been computed previously
	//Range range3 = thisColumn.removed; // Marked for removal, will be legally removed after this update cycle
	
	// There are two ways how column references can be passed to this class: 
	// - either by the class itself, for example, we always have access to Schema and hence can always resolve column names. 
	// - or are provided from outside when the system gets dependency information and we need to store these references wherever we want
	//   - the dependencies can be requested and column references injected from outside. The system anyway must get dependencies (which columns will be used) in order to build execution plan.  
	//   - the dependencies and column resolution can be requested from inside at any time.
	
	
	// We have to return the type that is expected in the declaration of the column:
	// - primitive data type
	// - table data type
	//   - rowid if we have found some record in the output table. currently, we do not have any means for that.
	//   - record object which will be pushed into the output table and the found rowid will be stored as this column value.
	//     - what the system/table will do with the pushed record is defined by the table (push method)
	//     - for export tables, the record will be pushed but null will be returned which we do not need. 
	//       it is a use case for columns which do not want to store their output values because they are not used
	//       these could be specially implemented export columns with mapping config as a function
	//       or the system could determine such columns automatically, for example, if their output is an export (leaf) table
	
	// We could also find a record in the output/type table instead of pushing it.

	public void endEvaluate();

}

/**
 * 
 */
class EvaluatorExpr implements Evaluator {
	public static String OUT_VARIABLE_NAME = "out";
	
	public boolean isExp4j() { return true; }
	public boolean isEvalex() { return false; }
	
	// All terms (dependencies) of the expression are paths starting from this table
	public Table table;
	
	// Formula
	protected String formula;

	// Will be filled by parser and then augmented by binder
	protected List<ExprDependency> exprDependencies = new ArrayList<ExprDependency>();

	// Native expressions produced during translation and used during evaluation
	protected net.objecthunter.exp4j.Expression exp4jExpression;
	protected com.udojava.evalex.Expression evalexExpression = null;

	//
	// EvaluatorExpr interface
	//
	@Override
	public void setParamPaths(List<String> paths) {
		; // TODO
	}
	@Override
	public List<QName> getParamPaths() { // TODO: Ensure that the first path is this column itself
		List<QName> paths = new ArrayList<QName>();
		for(ExprDependency dep : this.exprDependencies) {
			paths.add(dep.qname);
		}
		return paths;
	}
	@Override
	public List<Column> getDependencies() { // Extract all unique column objects used taking into account recursive-dependence via out or this column name and by removing duplicating columns
		List<Column> columns = new ArrayList<Column>();
		for(ExprDependency dep : this.exprDependencies) {
			if(dep.qname.names.size() == 1 && dep.qname.names.get(0).equalsIgnoreCase(ExprNode.OUT_VARIABLE_NAME)) {
				; // Do not add to dependencies
			}
			else {
				dep.columns.forEach(x -> { if(!columns.contains(x)) columns.add(x); }); // Each dependency is a path and different paths can included same segments
			}
		}
		return columns;
	}
	@Override
	public void translate(String formula) {
		this.formula = formula;
		if(this.formula == null || this.formula.isEmpty()) return;
		this.parse();
		this.bind();
		this.build();
	}
	@Override
	public Object evaluate(Object[] params) {
		
		//
		// Set parameters in native expressions
		//
		int paramNo = 0;
		for(ExprDependency dep : this.exprDependencies) {
			Object value = params[paramNo];
			try {
				if(this.isExp4j()) {
					this.exp4jExpression.setVariable(dep.paramName, ((Number)value).doubleValue());
				}
				else if(this.isEvalex()) {
					
				}
			}
			catch(Exception e) {
				;
			}
			finally {
				paramNo++;
			}
		}

		//
		// Evaluate the final (native) expression
		//
		Object ret = null;
		if(this.isExp4j()) {
			ret = this.exp4jExpression.evaluate();
		}
		else if(this.isEvalex()) {
			ret = this.evalexExpression.eval();
		}
		
		return ret;
	}

	//
	// Status of the previous operation performed
	//
	public DcError status;
	public EvaluatorExpr getErrorNode() { // Find first node with an error and return it. Otherwise, null
		if(this.status != null && this.status.code != DcErrorCode.NONE) {
			return this;
		}
		return null;
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
		
		this.status = new DcError(DcErrorCode.NONE, "Parsed successfully.", "");
	}

	//
	// Bind
	//

	// Resolve all symbols found after parsing relative to the input table
	public void bind() {
		//
		// Resolve each column path in the formula relative to the input table
		//
		for(ExprDependency dep : this.exprDependencies) {

			dep.columns = dep.qname.resolveColumns(this.table); // Try to really resolve symbol

			if(dep.columns == null || dep.columns.size() < dep.qname.names.size()) {
				this.status = new DcError(DcErrorCode.BIND_ERROR, "Cannot resolve columns.", "Error resolving columns " + dep.pathName);
				return;
			}
		}
	}

	//
	// Build (native expression that can be evaluated)
	//

	public void build() {
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
			this.status = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", e.getMessage());
			return null;
		}

		//
		// Validate
		//
		exp.setVariables(vals); // Validation requires variables to be set
		net.objecthunter.exp4j.ValidationResult res = exp.validate(); // Boolean argument can be used to ignore unknown variables
		if(!res.isValid()) {
			this.status = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", res.getErrors() != null && res.getErrors().size() > 0 ? res.getErrors().get(0) : "");
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
			this.status = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", e.getMessage());
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
			this.status = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", ee.getMessage());
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

	public EvaluatorExpr(Table tbl) {
		this.table = tbl;
	}
}

class ExprDependency {
	public int start;
	public int end;
	public String pathName;
	public String paramName;
	public QName qname;
	public List<Column> columns;
}
