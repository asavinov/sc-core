package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;

/**
 * User defined expression. It knows how to computing one output value given several input values.
 * Normally it is implemented by a user class programmatically or produced by a translator from some syntactic representation (formula). 
 */
public interface UDE {
	//public Table getMainTable(); // Do we need this? Normally it is available in the context
	//public Column getOutputColumn(); // Do we need this? Normally it is available in the context
	public List<List<Column>> getInputPaths();
	public Object evaluate(Object[] params);
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
class UdeExample implements UDE {
	
	// Native expressions which do real computations
	protected net.objecthunter.exp4j.Expression exp4jExpression;
	protected com.udojava.evalex.Expression evalexExpression;
	

	List<List<Column>> inputPaths = new ArrayList<List<Column>>();
	public List<List<Column>> getInputPaths() {
		return this.inputPaths;
	}

	@Override
	public Object evaluate(Object[] params) {
		return (double)params[0] + 1;
	}

	public UdeExample(List<List<Column>> inputPaths) { // Binding of parameter
		this.inputPaths.addAll(inputPaths);
	}
}

class UdeJava implements UDE {

	public static String OUT_VARIABLE_NAME = "out";
	
	public boolean isExp4j() { return true; }
	public boolean isEvalex() { return false; }
	
	// Formula
	protected String formula;

	protected boolean isEquality; // The formula is a single parameter without operations

	// Native expressions which do real computations
	protected net.objecthunter.exp4j.Expression exp4jExpression;
	protected com.udojava.evalex.Expression evalexExpression;

	// List of all parameter paths recognized by the parser
	private List<UdeJavaParameter> exprDependencies = new ArrayList<UdeJavaParameter>();

	@Override
	public Object evaluate(Object[] params) {
		return null;
	}
	List<List<Column>> inputPaths = new ArrayList<List<Column>>();
	@Override
	public List<List<Column>> getInputPaths() {
		return this.inputPaths;
	}

	public UdeJava(String formula) {
		this.formula = formula;
	}
}

class UdeJavaParameter {
	public int start;
	public int end;
	public String pathName;
	public String paramName;
	public QName qname;
}
