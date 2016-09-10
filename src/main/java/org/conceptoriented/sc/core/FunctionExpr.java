package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
public class FunctionExpr {
	
	//
	// Syntax and parsing
	//

	public String name;
	public String type;

	public String formula; // If the function returns a primitive value then it is computed using this formula

	public List<FunctionExpr> tuple = new ArrayList<FunctionExpr>(); // If the function is non-primitive, then its value is a combination
	public boolean isTerminal() { // Whether we can continue expression tree, that is, this node can be expanded
		if(tuple == null || tuple.size() == 0) return true;
		else return false;
	}
	
	public Column column;

	//
	// Parse
	//

	protected List<PrimExprDependency> primExprDependencies = new ArrayList<PrimExprDependency>();
	protected List<Column> getPrimExprColumnDependencies() { // Extract column objects (must be bound)
		List<Column> columns = new ArrayList<Column>();

		for(PrimExprDependency dep : primExprDependencies) {
			for(Column col : dep.columns) {
				if(!columns.contains(col)) // Each dependency is a path and different paths can included same segments
					columns.add(col);
			}
		}
		
		return columns;
	}

	public void parsePrimExpr() { // Find all occurrences of column paths in the primitive expression
		primExprDependencies = new ArrayList<PrimExprDependency>();

		if(formula == null || formula.isEmpty()) {
			return;
		}
		
		String ex =  "\\[(.*?)\\]";
		//String ex = "[\\[\\]]";
		Pattern p = Pattern.compile(ex,Pattern.DOTALL);
		Matcher matcher = p.matcher(formula);

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
		// If between two names there is only dot then combine them into one path
		//
		for(int i = 0; i < names.size(); i++) {
			if(i == names.size()-1) { // Last element does not have continuation
				primExprDependencies.add(names.get(i));
				break;
			}
			
			int thisEnd = names.get(i).end;
			int nextStart = names.get(i+1).start;
			
			if(formula.substring(thisEnd, nextStart).trim() == ".") { // There is continuation.
				names.get(i+1).start = names.get(i).start; // Attach this name to the next name as a prefix
			}
			else { // No continuation. Ready to copy as path.
				primExprDependencies.add(names.get(i));
			}
		}
	}
	
	/**
	 * Parse formula by possibly building a tree of expressions with primitive expressions in the leaves.
	 * Any assignment has well defined structure QName=( {sequence of assignments} | expression)
	 */
	public void parse(String exprString) {
		if(exprString == null || exprString.isEmpty()) return;
		
		this.name = "";
		this.formula = "";
		this.tuple.clear();

		//
		// Find equality
		//
		int eq = exprString.indexOf("=");
		
		if(eq < 0) return; // Syntax error

		//
		// Extract name
		//
		String name = exprString.substring(0, eq).trim();
		if(name.startsWith("[")) name = name.substring(1);
		if(name.endsWith("]")) name = name.substring(0,name.length()-1);
		
		this.name = name;

		//
		// Extract value (function formula)
		//
		String value = exprString.substring(eq+1).trim();

		int open = value.indexOf("{");
		int close = value.lastIndexOf("}");
		
		this.formula = value;

		if(open < 0 && close < 0) { // Primitive expression
			this.parsePrimExpr();
			return;
		}
		else if(open >= 0 && close >= 0 && open < close) { // Tuple - combination of assignments
			String sequence = value.substring(open+1, close).trim();

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
					return; // Syntax error: too many closing parentheses
				}
				
				// Check if it is a member separator
				if(sequence.charAt(i) == ';') {
					members.add(sequence.substring(previousSeparator+1, i));
					previousSeparator = i;
				}
			}
			members.add(sequence.substring(previousSeparator+1, sequence.length()));

			// Create a tuple for each member and parse it
			for(String member : members) {
				FunctionExpr childTuple = new FunctionExpr();
				childTuple.parse(member.trim());
				this.tuple.add(childTuple);
			}
			return;
		}
		else {
			return; // Syntax error
		}
	}

	//
	// Bind
	//

	public void bind(Column column) {
		this.column = column;
		
		if(this.isTerminal()) {
			this.bindPrimExpr();
			this.buildPrimExpr();
		}
		else {
			; // TODO: Resolve tuple and then recursion
		}
	}

	public void bindPrimExpr() { // Resolve column names used in the primitive expression (must be parsed)

		Table input = column.getInput();

		if(primExprDependencies == null) {
			return;
		}
		
		//
		// Resolve each column name in the path
		//
    	QNameBuilder parser = new QNameBuilder();
    	
		for(PrimExprDependency dep : primExprDependencies) {
			dep.pathName = formula.substring(dep.start, dep.end);
			dep.qname = parser.buildQName(dep.pathName);
			dep.columns = dep.qname.resolveColumns(input); // Really resolve symbol
		}
	}
	
	// It will be used during evaluation but we build it once
	protected String transformedComputeFormula;
	protected Expression computeExpression;

	public void buildPrimExpr() {
		//
		// Transform the expression by using new names and get an executable expression
		//
		StringBuffer buf = new StringBuffer(formula);
		for(int i = primExprDependencies.size()-1; i >= 0; i--) {
			PrimExprDependency dep = primExprDependencies.get(i);
			dep.paramName = "__p__"+i;
			buf.replace(dep.start, dep.end, dep.paramName);
		}
		
		transformedComputeFormula = buf.toString();

		//
		// Create expression object with the transformed formula
		//
		ExpressionBuilder builder = new ExpressionBuilder(transformedComputeFormula);
		Set<String> vars = primExprDependencies.stream().map(x -> x.paramName).collect(Collectors.toCollection(HashSet::new));
		builder.variables(vars);

		Expression exp = builder.build(); // Here we get parsing exceptions which might need be caught and processed
		
		ValidationResult res = exp.validate(); // Boolean argument can be used to ignore unknown variables
		res.isValid();
		
		computeExpression = exp;
	}

	//
	// Values and evaluation
	//

	public Object result; // Result of evaluation: either primitive value or record id

	protected void setPrimExprVariables(long i) {
		for(PrimExprDependency dep : primExprDependencies) {
			Object value = column.getValue(dep.columns, i);
			if(value == null) {
				value = Double.NaN;
			}
			computeExpression.setVariable(dep.paramName, ((Number)value).doubleValue()); // Pass these values into the expression
		}
	}

	public void evaluate(long i) {
		if(this.isTerminal()) {
			setPrimExprVariables(i); // For each input, read all necessary column values
			result = computeExpression.evaluate();
		}
		else {
			; // TODO:
		}
	}

	protected void evaluatePrimExpr() {
		
		Table input = column.getInput();
		
		// Evaluate for all rows in the (dirty, new) range
		Range range = input.getNewRange();
		for(long i=range.start; i<range.end; i++) {

			// For each input, read all necessary column values
			for(PrimExprDependency dep : primExprDependencies) {
				Object value = column.getValue(dep.columns, i);
				if(value == null) {
					value = Double.NaN;
				}
				computeExpression.setVariable(dep.paramName, ((Number)value).doubleValue()); // Pass these values into the expression
			}

			// Evaluate and get output value
			Double result = computeExpression.evaluate();

			// Store the output value for the current row
			column.setValue(i, result);
		}

	}


	public FunctionExpr() {
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
