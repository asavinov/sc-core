package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import net.objecthunter.exp4j.ValidationResult;

/**
 * It is a function definition with function name, function type and function formula. 
 * The formula can be a primitive expression or a tuple which is a combination of function formulas. 
 */
public class ExprNode {
	
	//
	// Formula belongs to either group table or fact table if it exists (in the case of aggregation)
	//
	public String formula; // Syntactic representation of the function (without name and equality - only the body)

	//
	// Parameters for any column
	//
	public String tableName; // Table where we define a new (aggregated) column
	public Table table;
	public String name; // (Aggregated) column name that is being defined
	public Column column;

	//
	// Parameters for accumulated column
	//
	public String accuformula;
	public String accutableName; // It is table we loop through (this table)
	public Table accutable; // Primitive expressions in the definition (body) belong to this type (corresponds to 'this' variable). It is resolved from the corresponding table name.
	public String accupathName; // It is a path from the fact table to the group table
	public QName accupathQName;
	public List<Column> accupath; // Path from fact table to group table (or empty). It is resolved from the syntactic path.

	// Determine if it is syntactically accumulation, that is, it seems to be intended for accumulation.
	// In fact, it can be viewed as a syntactic check of validity and can be called isValidAccumulation
	public boolean isAccumulation() {
		if(this.accuformula == null || this.accuformula.trim().isEmpty()) return false;
		if(this.accutableName == null || this.accutableName.trim().isEmpty()) return false;
		if(this.accupathName == null || this.accupathName.trim().isEmpty()) return false;

		if(this.accutableName.equalsIgnoreCase(this.tableName)) return false;

		return true;
	}

	//
	// Tuple expression
	//
	public List<ExprNode> children = new ArrayList<ExprNode>(); // If the function is non-primitive, then its value is a combination

	public Record childrenToRecord() {
		Record r = new Record();
		children.forEach(x -> r.set(x.name, x.result));
		return r;
	}
	
	// Determine if it is a tuple syntactically (it seems to be intended to be a tuple), that is, something enclosed in curly brackets
	// In fact, it can be viewed as a syntactic check of validity and can be called isValidTuple
	public boolean isTuple() {
		if(this.formula == null) return false;

		int open = this.formula.indexOf("{");
		int close = this.formula.lastIndexOf("}");
		
		if(open < 0 || close < 0) return false;

		return true;
	}

	//
	// Status of the previous operation performed
	//
	public DcError status;
	public ExprNode getErrorNode() { // Find first node with an error and return it. Otherwise, null
		if(this.status != null && this.status.code != DcErrorCode.NONE) {
			return this;
		}
		for(ExprNode node : children) {
			ExprNode result = node.getErrorNode();
			if(result != null) return result;
		}
		return null;
	}

	protected List<PrimExprDependency> primExprDependencies = new ArrayList<PrimExprDependency>(); // Will be filled by parser and then augmented by binder
	
	public List<Column> getDependencies() { // Extract all unique column objects used (must be bound)
		List<Column> columns = new ArrayList<Column>();
		if(!this.isTuple()) {
			for(PrimExprDependency dep : this.primExprDependencies) {
				if(dep.columns == null) continue; // Probably not yet resolved
				dep.columns.forEach(x -> { if(!columns.contains(x)) columns.add(x); }); // Each dependency is a path and different paths can included same segments
			}
		}
		else {
			// Collect from all children
			for(ExprNode expr : children) {
				List<Column> childDeps = expr.getDependencies();
				if(childDeps == null) continue;
				childDeps.forEach(x -> { if(!columns.contains(x)) columns.add(x); } );
			}
			// Are member names also dependencies? Does this function depend on its tuple member names?
		}
		return columns;
	}

	//
	// Parse
	//

	/**
	 * Parse formulas by possibly building a tree of expressions with primitive expressions in the leaves.
	 * Any assignment has well defined structure QName=( {sequence of assignments} | expression)
	 * The result of parsing is a list of symbols.
	 */
	public void parse() {
		if(this.formula == null || this.formula.isEmpty()) return;

		if(!this.isTuple()) { // Non-tuple: either calculated or aggregated
			this.primExprDependencies = new ArrayList<PrimExprDependency>();

			this.parsePrimExpr(this.formula); // Parse main formula relative to main table

			if(this.isAccumulation()) { // Parse accu formula relative to accu table
				this.parsePrimExpr(this.accuformula);
				this.parseAccupath();
			}

			return;
		}
		else { // Tuple - combination of assignments

			//
			// Check the enclosure (curly brackets)
			//
			int open = this.formula.indexOf("{");
			int close = this.formula.lastIndexOf("}");

			if(open < 0 || close < 0 || open >= close) {
				this.status = new DcError(DcErrorCode.PARSE_ERROR, "Problem with curly braces.", "Tuple expression is a list of assignments in curly braces.");
				return;
			}

			String sequence = this.formula.substring(open+1, close).trim();

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
					this.status = new DcError(DcErrorCode.PARSE_ERROR, "Problem with curly braces.", "Opening and closing curly braces must match each other.");
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
					this.status = new DcError(DcErrorCode.PARSE_ERROR, "No equality sign.", "Tuple expression is a list of assignments using equality sign.");
					return;
				}
				String left = member.substring(0, eq).trim();
				if(left.startsWith("[")) left = left.substring(1);
				if(left.endsWith("]")) left = left.substring(0,left.length()-1);
				String rigth = member.substring(eq+1).trim();

				ExprNode childTuple = new ExprNode();

				childTuple.formula = rigth;
				childTuple.name = left;

				childTuple.parse();
				if(childTuple.status != null && childTuple.status.code != DcErrorCode.NONE) { // Propagate child error to the parent
					this.status = childTuple.status;
					return;
				}

				this.children.add(childTuple);
			}

			this.status = new DcError(DcErrorCode.NONE, "Parsed successfully.", "");
		}
	}

	// Find all occurrences of column paths in the primitive expression
	public void parsePrimExpr(String frml) {
		if(frml == null || frml.isEmpty()) return;
		
		String ex =  "\\[(.*?)\\]";
		//String ex = "[\\[\\]]";
		Pattern p = Pattern.compile(ex,Pattern.DOTALL);
		Matcher matcher = p.matcher(frml);

		List<PrimExprDependency> names = new ArrayList<PrimExprDependency>();
		while(matcher.find())
		{
			int s = matcher.start();
			int e = matcher.end();
			String name = matcher.group();
			PrimExprDependency entry = new PrimExprDependency();
			entry.start = s;
			entry.end = e;
			names.add(entry);
		}
		
		//
		// Create column paths by concatenating dot separated name sequences
		//
		for(int i = 0; i < names.size(); i++) {
			if(i == names.size()-1) { // Last element does not have continuation
				this.primExprDependencies.add(names.get(i));
				break;
			}
			
			int thisEnd = names.get(i).end;
			int nextStart = names.get(i+1).start;
			
			if(frml.substring(thisEnd, nextStart).trim() == ".") { // There is continuation.
				names.get(i+1).start = names.get(i).start; // Attach this name to the next name as a prefix
			}
			else { // No continuation. Ready to copy as path.
				this.primExprDependencies.add(names.get(i));
			}
		}

    	//
		// Process the column paths
		//
		QNameBuilder parser = new QNameBuilder();

		for(PrimExprDependency dep : this.primExprDependencies) {
			dep.pathName = frml.substring(dep.start, dep.end);
			dep.qname = parser.buildQName(dep.pathName);
		}
		
		this.status = new DcError(DcErrorCode.NONE, "Parsed successfully.", "");
	}
	
	public void parseAccupath() {
		QNameBuilder parser = new QNameBuilder();

		// this.accutableName has to be correct table name
		// this.accupathName has to be correct column path
		this.accupathQName = parser.buildQName(this.accupathName);
		if(this.accupathQName == null || this.accupathQName.names.size() == 0) {
			this.status = new DcError(DcErrorCode.PARSE_ERROR, "Syntax error in group path.", "Error group path: '" + this.accupathName + "'");
			return;
		}

		// TODO: We need to add these dependencies into dependence graph along with accu measures
	}

	//
	// Bind
	//

	public void bind() {
		
		if(!this.isTuple()) { // Non-tuple: either calculated or aggregated

			this.bindPrimExpr(); // Bind main formula relative to main table

			if(this.isAccumulation()) { // Bind accu formula relative to accu table
				//this.parsePrimExpr(this.accuformula);
				//this.parseAccupath();
			}

			return;
		}
		else { // Tuple - combination of assignments
			Table output = column.getOutput();

			for(ExprNode expr : children) {
				Column col = output.getSchema().getColumn(output.getName(), expr.name); // Really resolve name as a column in our type table
				if(col != null) {
					expr.column = col;
				}
				else {
					this.status = new DcError(DcErrorCode.BIND_ERROR, "Column name not found.", "Error finding column with the name [" + expr.name + "]");
					return;
				}
				
				expr.table = this.table; // Parent 'this' will be used by all child expressions
				expr.bind(); // Recursion
			}

			this.status = new DcError(DcErrorCode.NONE, "Resolved successfully.", "");
		}
	}

	public void bindPrimExpr() { // Resolve column names used in the primitive expression (must be parsed)

		if(this.primExprDependencies == null) {
			return;
		}
		
		Schema schema = table.getSchema();

		//
		// Choose which of two tables will be a looping table the formula will be applied to and resolved from
		//
		Table looptable = this.table;
		if(this.isAccumulation()) {

			this.accutable = schema.getTable(accutableName); // Resolve fact table name
			if(this.accutable == null) {
				this.status = new DcError(DcErrorCode.BIND_ERROR, "Cannot resolve table.", "Error resolving table " + accutableName);
				return;
			}
			looptable = this.accutable;

			this.accupath = this.accupathQName.resolveColumns(this.accutable); // Try to really resolve symbol
			if(this.accupath == null || this.accupath.size() < this.accupathQName.names.size()) {
				this.status = new DcError(DcErrorCode.BIND_ERROR, "Cannot resolve columns.", "Error resolving column " + this.accupathName);
				return;
			}
		}

		//
		// Resolve each column path in the formula relative to the loop table
		//
		for(PrimExprDependency dep : this.primExprDependencies) {

			dep.columns = dep.qname.resolveColumns(looptable); // Try to really resolve symbol

			if(dep.columns == null || dep.columns.size() < dep.qname.names.size()) {
				this.status = new DcError(DcErrorCode.BIND_ERROR, "Cannot resolve columns.", "Error resolving columns " + dep.pathName);
				return;
			}
		}
		
		//
		// Parse and bind the final (native) expression
		//
		buildPrimExpr();
	}
	
	//
	// Evaluate
	//

	// It will be used during evaluation but we build it once
	protected String transformedComputeFormula;
	protected Expression computeExpression;

	public void buildPrimExpr() {
		//
		// Transform the expression by using new names and get an executable expression
		//
		StringBuffer buf = new StringBuffer(formula);
		for(int i = this.primExprDependencies.size()-1; i >= 0; i--) {
			PrimExprDependency dep = this.primExprDependencies.get(i);
			dep.paramName = "__p__"+i;
			buf.replace(dep.start, dep.end, dep.paramName);
		}
		
		this.transformedComputeFormula = buf.toString();

		//
		// Create a list of variables used in the expression
		//
		Set<String> vars = new HashSet<String>();
		Map<String, Double> vals = new HashMap<String, Double>();
		for(PrimExprDependency dep : this.primExprDependencies) {
			vars.add(dep.paramName);
			vals.put(dep.paramName, 0.0);
		}
		// Set<String> vars = this.primExprDependencies.stream().map(x -> x.paramName).collect(Collectors.toCollection(HashSet::new));
		
		// Add the current output value as a special (reserved) variable
		vars.add("output");
		vals.put("output", 0.0);

		//
		// Create expression object with the transformed formula
		//
		Expression exp = null;
		try {
			ExpressionBuilder builder = new ExpressionBuilder(this.transformedComputeFormula);
			builder.variables(vars);
			exp = builder.build(); // Here we get parsing exceptions which might need be caught and processed
		}
		catch(Exception e) {
			this.status = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", e.getMessage());
			this.computeExpression = null;
			return;
		}
		exp.setVariables(vals);
		
		ValidationResult res = exp.validate(); // Boolean argument can be used to ignore unknown variables
		if(!res.isValid()) {
			this.status = new DcError(DcErrorCode.PARSE_ERROR, "Expression error.", res.getErrors() != null && res.getErrors().size() > 0 ? res.getErrors().get(0) : "");
			this.computeExpression = null;
		}
		{
			this.computeExpression = exp;
		}
	}

	public Object result; // Result of evaluation: either primitive value or record id

	public void beginEvaluate() {
		if(!this.isTuple()) {
			this.buildPrimExpr();
		}
		else {
			for(ExprNode expr : children) {
				expr.beginEvaluate(); // Recursion
			}
		}
	}

	protected void setPrimExprVariables(long i) { // Pass all variable values for the specified input to the compute expression
		for(PrimExprDependency dep : this.primExprDependencies) {
			Object value = dep.columns.get(0).getValue(dep.columns, i);
			if(value == null) value = Double.NaN;
			this.computeExpression.setVariable(dep.paramName, ((Number)value).doubleValue());
		}
		
		// Set current output value as a special variable.
		Object outputValue;
		if(!this.isAccumulation()) {
			outputValue = column.getValue(i);
		}
		else {
			long g = (Long) this.accupath.get(0).getValue(this.accupath, i); // Find group element
			outputValue = column.getValue(g);
		}
		if(outputValue == null) outputValue = Double.NaN;
		this.computeExpression.setVariable("output", ((Number)outputValue).doubleValue());
	}

	public void evaluate(long i) {
		if(!this.isTuple()) { // Primitive expression
			setPrimExprVariables(i); // For each input, read all necessary column values from fact table and the current output from the group table
			result = this.computeExpression.evaluate();
		}
		else { // Tuple
			// Evaluation recursion down to primitive expressions which compute primitive values
			for(ExprNode expr : children) {
				expr.evaluate(i);
			}
			// After recursion, members are supposed to store result values
			
			Table output = column.getOutput();
			Record r = this.childrenToRecord(); // Output value is this record
			long row = output.find(r, true); // But we store a record reference so find it
			result = row; // We store id of the record - not the record itself
		}
	}

	
	public ExprNode() {
	}
}

class PrimExprDependency {
	public int start;
	public int end;
	public String pathName;
	public String paramName;
	public QName qname;
	public List<Column> columns;
}
